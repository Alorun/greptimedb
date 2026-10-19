// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use api::v1::meta::heartbeat_client::HeartbeatClient;
use api::v1::meta::{HeartbeatRequest, HeartbeatResponse, RequestHeader, Role};
use common_grpc::channel_manager::ChannelManager;
use common_meta::distributed_time_constants::{BASE_HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT};
use common_meta::{heartbeat, util};
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::{info, warn};
use snafu::{OptionExt, ResultExt, ensure};
use tokio::sync::{RwLock, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Streaming;
use tonic::transport::Channel;

use crate::client::{Id, LeaderProviderRef};
use crate::error;
use crate::error::{InvalidResponseHeaderSnafu, Result};

/// Heartbeat configuration received from Metasrv during handshake.
#[derive(Debug, Clone, Copy)]
pub struct HeartbeatConfig {
    pub interval: Duration,
    pub retry_interval: Duration,
    pub gc_enabled: bool,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            interval: BASE_HEARTBEAT_INTERVAL,
            retry_interval: BASE_HEARTBEAT_INTERVAL,
            gc_enabled: false,
        }
    }
}

impl fmt::Display for HeartbeatConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "interval={:?}, retry={:?}, gc_enabled={}",
            self.interval, self.retry_interval, self.gc_enabled
        )
    }
}

impl HeartbeatConfig {
    /// Extract configuration from HeartbeatResponse.
    pub fn from_response(res: &HeartbeatResponse) -> Self {
        if let Some(cfg) = &res.heartbeat_config {
            // Metasrv provided complete configuration
            Self {
                interval: Duration::from_millis(cfg.heartbeat_interval_ms),
                retry_interval: Duration::from_millis(cfg.retry_interval_ms),
                gc_enabled: cfg.gc_enabled,
            }
        } else {
            let fallback = Self::default();
            warn!(
                "Metasrv didn't provide heartbeat_config, using default: {}",
                fallback
            );
            fallback
        }
    }
}

pub struct HeartbeatSender {
    id: Id,
    role: Role,
    sender: mpsc::Sender<HeartbeatRequest>,
    compact_node_info: bool,
    node_info_sent: AtomicBool,
}

impl HeartbeatSender {
    #[inline]
    fn new(
        id: Id,
        role: Role,
        sender: mpsc::Sender<HeartbeatRequest>,
        compact_node_info: bool,
    ) -> Self {
        Self {
            id,
            role,
            sender,
            compact_node_info,
            node_info_sent: AtomicBool::new(false),
        }
    }

    #[inline]
    pub fn id(&self) -> Id {
        self.id
    }

    #[inline]
    pub async fn send(&self, mut req: HeartbeatRequest) -> Result<()> {
        req.set_header(
            self.id,
            self.role,
            TracingContext::from_current_span().to_w3c(),
        );
        let has_node_info = req.info.is_some();
        if self.compact_node_info
            && self.node_info_sent.load(Ordering::Acquire)
            && let Some(info) = req.info.as_mut()
        {
            info.version.clear();
            info.git_commit.clear();
            info.start_time_ms = 0;
        }
        self.sender.send(req).await.map_err(|e| {
            error::SendHeartbeatSnafu {
                err_msg: e.to_string(),
            }
            .build()
        })?;
        // A full request must enter this stream's queue before any sender can
        // omit these fields. Concurrent first sends may safely repeat them.
        if has_node_info {
            self.node_info_sent.store(true, Ordering::Release);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct HeartbeatStream {
    id: Id,
    stream: Streaming<HeartbeatResponse>,
}

impl HeartbeatStream {
    #[inline]
    fn new(id: Id, stream: Streaming<HeartbeatResponse>) -> Self {
        Self { id, stream }
    }

    #[inline]
    pub fn id(&self) -> Id {
        self.id
    }

    /// Fetch the next message from this stream.
    #[inline]
    pub async fn message(&mut self) -> Result<Option<HeartbeatResponse>> {
        let res = self.stream.message().await.map_err(error::Error::from);
        if let Ok(Some(heartbeat)) = &res {
            util::check_response_header(heartbeat.header.as_ref())
                .context(InvalidResponseHeaderSnafu)?;
        }
        res
    }
}

#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<RwLock<Inner>>,
}

impl Client {
    pub fn new(id: Id, role: Role, channel_manager: ChannelManager) -> Self {
        let inner = Arc::new(RwLock::new(Inner::new(id, role, channel_manager)));
        Self { inner }
    }

    /// Start the client with a [LeaderProvider].
    pub(crate) async fn start_with(&self, leader_provider: LeaderProviderRef) -> Result<()> {
        let mut inner = self.inner.write().await;
        inner.start_with(leader_provider)
    }

    pub async fn ask_leader(&mut self) -> Result<String> {
        let inner = self.inner.read().await;
        inner.ask_leader().await
    }

    pub async fn heartbeat(
        &mut self,
    ) -> Result<(HeartbeatSender, HeartbeatStream, HeartbeatConfig)> {
        let inner = self.inner.read().await;
        inner.ask_leader().await?;
        inner.heartbeat().await
    }
}

#[derive(Debug)]
struct Inner {
    id: Id,
    role: Role,
    channel_manager: ChannelManager,
    leader_provider: Option<LeaderProviderRef>,
}

impl Inner {
    fn new(id: Id, role: Role, channel_manager: ChannelManager) -> Self {
        Self {
            id,
            role,
            channel_manager,
            leader_provider: None,
        }
    }

    fn start_with(&mut self, leader_provider: LeaderProviderRef) -> Result<()> {
        ensure!(
            !self.is_started(),
            error::IllegalGrpcClientStateSnafu {
                err_msg: "Heartbeat client already started"
            }
        );
        self.leader_provider = Some(leader_provider);
        Ok(())
    }

    async fn ask_leader(&self) -> Result<String> {
        let Some(leader_provider) = self.leader_provider.as_ref() else {
            return error::IllegalGrpcClientStateSnafu {
                err_msg: "not started",
            }
            .fail();
        };
        leader_provider.ask_leader().await
    }

    async fn heartbeat(&self) -> Result<(HeartbeatSender, HeartbeatStream, HeartbeatConfig)> {
        ensure!(
            self.is_started(),
            error::IllegalGrpcClientStateSnafu {
                err_msg: "Heartbeat client not start"
            }
        );

        let leader_addr = self
            .leader_provider
            .as_ref()
            .unwrap()
            .leader()
            .context(error::NoLeaderSnafu)?;
        let mut leader = self.make_client(&leader_addr)?;

        let (sender, receiver) = mpsc::channel::<HeartbeatRequest>(128);

        let header = RequestHeader::new(
            self.id,
            self.role,
            TracingContext::from_current_span().to_w3c(),
        );
        let mut handshake = HeartbeatRequest {
            header: Some(header),
            ..Default::default()
        };
        handshake
            .extensions
            .insert(heartbeat::COMPACT_NODE_INFO_KEY.to_string(), b"1".to_vec());
        sender.send(handshake).await.map_err(|e| {
            error::SendHeartbeatSnafu {
                err_msg: e.to_string(),
            }
            .build()
        })?;
        let receiver = ReceiverStream::new(receiver);

        let handshake = async {
            let mut stream = leader
                .heartbeat(receiver)
                .await
                .map_err(error::Error::from)?
                .into_inner();

            let res = stream
                .message()
                .await
                .map_err(error::Error::from)?
                .context(error::CreateHeartbeatStreamSnafu)?;
            Ok::<_, error::Error>((stream, res))
        };

        let (stream, res) = tokio::time::timeout(HEARTBEAT_TIMEOUT, handshake)
            .await
            .with_context(|_| error::HeartbeatHandshakeTimeoutSnafu {
                timeout: HEARTBEAT_TIMEOUT,
            })??;

        let config = HeartbeatConfig::from_response(&res);
        let compact_node_info = res
            .extensions
            .get(heartbeat::COMPACT_NODE_INFO_KEY)
            .is_some_and(|value| value == b"1");

        info!(
            "Handshake successful with Metasrv at {}, received config: {}",
            leader_addr, config
        );

        Ok((
            HeartbeatSender::new(self.id, self.role, sender, compact_node_info),
            HeartbeatStream::new(self.id, stream),
            config,
        ))
    }

    fn make_client(&self, addr: impl AsRef<str>) -> Result<HeartbeatClient<Channel>> {
        let channel = self
            .channel_manager
            .get(addr)
            .context(error::CreateChannelSnafu)?;

        Ok(common_grpc::configure_tonic_client!(
            HeartbeatClient::new(channel),
            self.channel_manager,
        ))
    }

    #[inline]
    pub(crate) fn is_started(&self) -> bool {
        self.leader_provider.is_some()
    }
}

#[cfg(test)]
mod tests {
    use api::v1::meta::{NodeInfo as PbNodeInfo, Peer};
    use common_meta::cluster::{NodeInfo, NodeInfoKey};
    use common_meta::kv_backend::ResettableKvBackendRef;

    use super::*;
    use crate::client::AskLeader;

    struct HeartbeatTest {
        sender: HeartbeatSender,
        requests: mpsc::Receiver<HeartbeatRequest>,
        transport: mpsc::Sender<HeartbeatRequest>,
        responses: HeartbeatStream,
        in_memory: ResettableKvBackendRef,
    }

    impl HeartbeatTest {
        async fn new() -> Self {
            let (client, ctx) = crate::mocks::mock_client_with_memstore().await;
            let (mut sender, responses, _) = client.heartbeat().await.unwrap();
            let (tx, requests) = mpsc::channel(8);
            let transport = std::mem::replace(&mut sender.sender, tx);
            Self {
                sender,
                requests,
                transport,
                responses,
                in_memory: ctx.in_memory.unwrap(),
            }
        }

        async fn capture(&mut self, request: HeartbeatRequest) -> HeartbeatRequest {
            self.sender.send(request).await.unwrap();
            self.requests.recv().await.unwrap()
        }

        async fn forward(&mut self, request: HeartbeatRequest) {
            self.transport.send(request).await.unwrap();
            self.response().await;
        }

        async fn response(&mut self) {
            // Address registration can also push mailbox messages on this stream.
            loop {
                let response = self.responses.message().await.unwrap().unwrap();
                if response.mailbox_message.is_none() {
                    return;
                }
            }
        }

        async fn node_info(&self) -> NodeInfo {
            let key: Vec<u8> = (&NodeInfoKey::new(&node_heartbeat(0, 0, "")).unwrap()).into();
            NodeInfo::try_from(self.in_memory.get(&key).await.unwrap().unwrap().value).unwrap()
        }
    }

    fn node_heartbeat(cpu: i64, memory: i64, hostname: &str) -> HeartbeatRequest {
        HeartbeatRequest {
            header: Some(RequestHeader::new(2000, Role::Datanode, Default::default())),
            peer: Some(Peer {
                id: 2000,
                addr: "node:3001".into(),
            }),
            node_epoch: 100,
            info: Some(PbNodeInfo {
                version: "v1".into(),
                git_commit: "abc".into(),
                start_time_ms: 100,
                cpu_usage_millicores: cpu,
                memory_usage_bytes: memory,
                hostname: hostname.into(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn assert_updated_node(info: NodeInfo) {
        assert_eq!(info.version, "v1");
        assert_eq!(info.git_commit, "abc");
        assert_eq!(info.start_time_ms, 100);
        assert_eq!(info.cpu_usage_millicores, 0);
        assert_eq!(info.memory_usage_bytes, 800);
        assert_eq!(info.hostname, "host-b");
    }

    #[tokio::test]
    async fn test_compact_node_info_preserves_node_updates() {
        let mut test = HeartbeatTest::new().await;
        let first = node_heartbeat(200, 500, "host-a");
        let request = test.capture(first.clone()).await;
        assert_eq!(request.info, first.info);
        test.forward(request).await;

        let request = test.capture(node_heartbeat(0, 800, "host-b")).await;
        assert_eq!(
            request.info,
            Some(PbNodeInfo {
                memory_usage_bytes: 800,
                hostname: "host-b".into(),
                ..Default::default()
            })
        );
        test.forward(request).await;
        assert_updated_node(test.node_info().await);
    }

    #[tokio::test]
    async fn test_control_message_preserves_first_full_node_info() {
        let mut test = HeartbeatTest::new().await;
        let request = test.capture(HeartbeatRequest::default()).await;
        test.forward(request).await;

        let first = node_heartbeat(200, 500, "host-a");
        let request = test.capture(first.clone()).await;
        assert_eq!(request.info, first.info);
        test.forward(request).await;

        let request = test.capture(node_heartbeat(0, 800, "host-b")).await;
        assert_eq!(request.info.as_ref().unwrap().version, "");
        test.forward(request).await;
        assert_updated_node(test.node_info().await);
    }

    #[tokio::test]
    async fn test_compact_node_info_does_not_wait_for_response() {
        let mut test = HeartbeatTest::new().await;
        // Neither request reaches Metasrv until both sends have completed.
        let requests = tokio::time::timeout(Duration::from_secs(1), async {
            let first = test.capture(node_heartbeat(200, 500, "host-a")).await;
            let second = test.capture(node_heartbeat(0, 800, "host-b")).await;
            [first, second]
        })
        .await
        .unwrap();
        for request in requests {
            test.transport.send(request).await.unwrap();
        }
        for _ in 0..2 {
            test.response().await;
        }
        assert_updated_node(test.node_info().await);
    }

    #[tokio::test]
    async fn test_already_start() {
        let client = Client::new(0, Role::Datanode, ChannelManager::default());
        let leader_provider = Arc::new(AskLeader::new(
            0,
            Role::Datanode,
            vec!["127.0.0.1:1000".to_string(), "127.0.0.1:1001".to_string()],
            ChannelManager::default(),
            3,
        ));
        client.start_with(leader_provider.clone()).await.unwrap();
        let res = client.start_with(leader_provider).await;
        assert!(res.is_err());
        assert!(matches!(
            res.err(),
            Some(error::Error::IllegalGrpcClientState { .. })
        ));
    }

    #[tokio::test]
    async fn test_heartbeat_stream() {
        let (sender, mut receiver) = mpsc::channel::<HeartbeatRequest>(100);
        let sender = HeartbeatSender::new(8, Role::Datanode, sender, false);
        let _handle = tokio::spawn(async move {
            for _ in 0..10 {
                sender
                    .send(node_heartbeat(200, 500, "host-a"))
                    .await
                    .unwrap();
            }
        });
        while let Some(req) = receiver.recv().await {
            let header = req.header.unwrap();
            assert_eq!(8, header.member_id);
            assert_eq!(req.info, node_heartbeat(200, 500, "host-a").info);
        }
    }
}
