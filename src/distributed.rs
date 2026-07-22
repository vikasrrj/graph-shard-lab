use std::collections::{BTreeMap, HashSet};
use tokio::sync::{mpsc, oneshot};

use crate::Graph;

#[derive(Debug, PartialEq, Eq)]
pub struct DistributedQueryResult {
    pub user_ids: Vec<u64>,
    pub shard_requests: usize,
}

enum ShardCommand {
    AddUser {
        id: u64,
        name: String,
        reply: oneshot::Sender<Result<(), String>>,
    },

    AddFollow {
        source: u64,
        target: u64,
        reply: oneshot::Sender<Result<(), String>>,
    },

    GetFollowing {
        source: u64,
        reply: oneshot::Sender<Vec<u64>>,
    },

    BatchGetFollowing {
        sources: Vec<u64>,
        reply: oneshot::Sender<Vec<(u64, Vec<u64>)>>,
    },
}

#[derive(Clone)]
struct ShardHandle {
    shard_id: usize,
    sender: mpsc::Sender<ShardCommand>,
}

impl ShardHandle {
    async fn add_user(&self, id: u64, name: String) -> Result<(), String> {
        let (reply_sender, reply_receiver) = oneshot::channel();

        self.sender
            .send(ShardCommand::AddUser {
                id,
                name,
                reply: reply_sender,
            })
            .await
            .map_err(|_| format!("Shard worker {} has stopped", self.shard_id))?;

        reply_receiver.await.map_err(|_| {
            format!(
                "Shard worker {} dropped the add-user response",
                self.shard_id
            )
        })?
    }

    async fn add_follow(&self, source: u64, target: u64) -> Result<(), String> {
        let (reply_sender, reply_receiver) = oneshot::channel();

        self.sender
            .send(ShardCommand::AddFollow {
                source,
                target,
                reply: reply_sender,
            })
            .await
            .map_err(|_| format!("Shard worker {} has stopped", self.shard_id))?;

        reply_receiver.await.map_err(|_| {
            format!(
                "Shard worker {} dropped the add-follow response",
                self.shard_id
            )
        })?
    }

    async fn get_following(&self, source: u64) -> Result<Vec<u64>, String> {
        let (reply_sender, reply_receiver) = oneshot::channel();

        self.sender
            .send(ShardCommand::GetFollowing {
                source,
                reply: reply_sender,
            })
            .await
            .map_err(|_| format!("Shard worker {} has stopped", self.shard_id))?;

        reply_receiver.await.map_err(|_| {
            format!(
                "Shard worker {} dropped the adjacency response",
                self.shard_id
            )
        })
    }
    async fn get_following_batch(&self, sources: Vec<u64>) -> Result<Vec<(u64, Vec<u64>)>, String> {
        let (reply_sender, reply_receiver) = oneshot::channel();

        self.sender
            .send(ShardCommand::BatchGetFollowing {
                sources,
                reply: reply_sender,
            })
            .await
            .map_err(|_| format!("Shard worker {} has stopped", self.shard_id))?;

        reply_receiver
            .await
            .map_err(|_| format!("Shard worker {} dropped the batch response", self.shard_id))
    }
}

fn spawn_shard_worker(shard_id: usize, channel_capacity: usize) -> ShardHandle {
    let (sender, mut receiver) = mpsc::channel::<ShardCommand>(channel_capacity);

    tokio::spawn(async move {
        let mut graph = Graph::new();

        while let Some(command) = receiver.recv().await {
            match command {
                ShardCommand::AddUser { id, name, reply } => {
                    let result = graph.add_user(id, &name);
                    let _ = reply.send(result);
                }

                ShardCommand::AddFollow {
                    source,
                    target,
                    reply,
                } => {
                    /*
                    Only the source user must exist on this shard.

                    The target user may belong to another shard.
                    */
                    let result = graph.add_follow_unchecked(source, target);

                    let _ = reply.send(result);
                }

                ShardCommand::GetFollowing { source, reply } => {
                    let adjacency_list = graph.get_following_ids(source).to_vec();

                    let _ = reply.send(adjacency_list);
                }

                ShardCommand::BatchGetFollowing { sources, reply } => {
                    let adjacency_lists = sources
                        .into_iter()
                        .map(|source| {
                            let adjacency_list = graph.get_following_ids(source).to_vec();

                            (source, adjacency_list)
                        })
                        .collect();

                    let _ = reply.send(adjacency_lists);
                }
            }
        }
    });

    ShardHandle { shard_id, sender }
}

pub struct DistributedShardedGraph {
    workers: Vec<ShardHandle>,

    /*
    The coordinator tracks globally known users.

    Individual workers only store users assigned to their shard.
    */
    users: HashSet<u64>,
}

impl DistributedShardedGraph {
    pub fn new(shard_count: usize, channel_capacity: usize) -> Result<Self, String> {
        if shard_count == 0 {
            return Err("Shard count must be greater than zero".to_string());
        }

        if channel_capacity == 0 {
            return Err("Channel capacity must be greater than zero".to_string());
        }

        let workers = (0..shard_count)
            .map(|shard_id| spawn_shard_worker(shard_id, channel_capacity))
            .collect();

        Ok(Self {
            workers,
            users: HashSet::new(),
        })
    }

    pub fn shard_count(&self) -> usize {
        self.workers.len()
    }

    pub fn shard_for(&self, user_id: u64) -> Option<usize> {
        if user_id == 0 {
            return None;
        }

        Some(user_id as usize % self.workers.len())
    }

    pub async fn add_user(&mut self, id: u64, name: &str) -> Result<(), String> {
        if id == 0 {
            return Err("User ID must be greater than zero".to_string());
        }

        if self.users.contains(&id) {
            return Err(format!("User {id} already exists"));
        }

        let shard_id = self
            .shard_for(id)
            .ok_or_else(|| format!("Cannot find shard for user {id}"))?;

        self.workers[shard_id]
            .add_user(id, name.to_string())
            .await?;

        self.users.insert(id);

        Ok(())
    }

    pub async fn add_follow(&self, source: u64, target: u64) -> Result<(), String> {
        if !self.users.contains(&source) {
            return Err(format!("Source user {source} does not exist"));
        }

        if !self.users.contains(&target) {
            return Err(format!("Target user {target} does not exist"));
        }

        let source_shard = self
            .shard_for(source)
            .ok_or_else(|| format!("Cannot find shard for user {source}"))?;

        self.workers[source_shard].add_follow(source, target).await
    }

    pub async fn get_following_ids(&self, source: u64) -> Result<Vec<u64>, String> {
        if !self.users.contains(&source) {
            return Err(format!("User {source} does not exist"));
        }

        let shard_id = self
            .shard_for(source)
            .ok_or_else(|| format!("Cannot find shard for user {source}"))?;

        self.workers[shard_id].get_following(source).await
    }

    pub async fn get_two_hop(&self, source: u64) -> Result<DistributedQueryResult, String> {
        let first_hops = self.get_following_ids(source).await?;

        let mut user_ids = Vec::new();
        let mut seen_users = HashSet::new();

        // Reading the source adjacency list is one request.
        let mut shard_requests = 1;

        for first_hop in first_hops {
            let second_hops = self.get_following_ids(first_hop).await?;

            shard_requests += 1;

            for second_hop in second_hops {
                if second_hop != source && seen_users.insert(second_hop) {
                    user_ids.push(second_hop);
                }
            }
        }

        user_ids.sort_unstable();

        Ok(DistributedQueryResult {
            user_ids,
            shard_requests,
        })
    }

    pub async fn get_two_hop_batched(&self, source: u64) -> Result<DistributedQueryResult, String> {
        let first_hops = self.get_following_ids(source).await?;

        /*
        Group first-hop users by owning shard.

        Each group becomes one actual channel message.
        */
        let mut batches: BTreeMap<usize, Vec<u64>> = BTreeMap::new();

        for first_hop in first_hops {
            let shard_id = self
                .shard_for(first_hop)
                .ok_or_else(|| format!("Cannot find shard for user {first_hop}"))?;

            batches.entry(shard_id).or_default().push(first_hop);
        }

        let mut user_ids = Vec::new();
        let mut seen_users = HashSet::new();

        /*
        One message reads the source adjacency list.

        After that, one batch message is sent to each distinct
        shard containing first-hop users.
        */
        let shard_requests = 1 + batches.len();

        for (shard_id, sources) in batches {
            let adjacency_lists = self.workers[shard_id].get_following_batch(sources).await?;

            for (_first_hop, second_hops) in adjacency_lists {
                for second_hop in second_hops {
                    if second_hop != source && seen_users.insert(second_hop) {
                        user_ids.push(second_hop);
                    }
                }
            }
        }

        user_ids.sort_unstable();

        Ok(DistributedQueryResult {
            user_ids,
            shard_requests,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shard_workers_execute_two_hop_query_through_channels() {
        let mut graph = DistributedShardedGraph::new(2, 32).unwrap();

        for id in 1..=5 {
            graph.add_user(id, &format!("user-{id}")).await.unwrap();
        }

        graph.add_follow(1, 2).await.unwrap();
        graph.add_follow(1, 3).await.unwrap();

        graph.add_follow(2, 4).await.unwrap();
        graph.add_follow(3, 5).await.unwrap();

        let result = graph.get_two_hop(1).await.unwrap();

        assert_eq!(result.user_ids, vec![4, 5]);

        /*
        One request for User 1, then one request for each
        first-hop user: Users 2 and 3.
        */
        assert_eq!(result.shard_requests, 3);
    }

    #[tokio::test]
    async fn batched_query_sends_one_message_per_target_shard() {
        let mut graph = DistributedShardedGraph::new(3, 32).unwrap();

        for id in 1..=10 {
            graph.add_user(id, &format!("user-{id}")).await.unwrap();
        }

        /*
        Users 2 and 5 belong to Shard 2.
        Users 3 and 6 belong to Shard 0.
        */
        graph.add_follow(1, 2).await.unwrap();
        graph.add_follow(1, 5).await.unwrap();
        graph.add_follow(1, 3).await.unwrap();
        graph.add_follow(1, 6).await.unwrap();

        graph.add_follow(2, 7).await.unwrap();
        graph.add_follow(5, 8).await.unwrap();
        graph.add_follow(3, 9).await.unwrap();
        graph.add_follow(6, 10).await.unwrap();

        let direct = graph.get_two_hop(1).await.unwrap();

        let batched = graph.get_two_hop_batched(1).await.unwrap();

        assert_eq!(direct.user_ids, vec![7, 8, 9, 10]);
        assert_eq!(batched.user_ids, direct.user_ids);

        // Source request plus four individual first-hop requests.
        assert_eq!(direct.shard_requests, 5);

        // Source request plus one message to each of two shards.
        assert_eq!(batched.shard_requests, 3);
    }

    #[tokio::test]
    async fn source_shard_stores_cross_shard_edge() {
        let mut graph = DistributedShardedGraph::new(2, 32).unwrap();

        graph.add_user(1, "Alice").await.unwrap();
        graph.add_user(2, "Bob").await.unwrap();

        assert_ne!(graph.shard_for(1), graph.shard_for(2),);

        graph.add_follow(1, 2).await.unwrap();

        assert_eq!(graph.get_following_ids(1).await.unwrap(), vec![2],);
    }

    #[test]
    fn rejects_invalid_worker_configuration() {
        assert!(DistributedShardedGraph::new(0, 32).is_err());

        assert!(DistributedShardedGraph::new(4, 0).is_err());
    }
}
