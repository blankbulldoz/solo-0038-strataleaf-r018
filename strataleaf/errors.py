class DatabaseError(Exception):
    """Base error for caller-visible database failures."""

class InvalidDocument(DatabaseError):
    pass

class Conflict(DatabaseError):
    pass

class NotFound(DatabaseError):
    pass

class SnapshotTooOld(DatabaseError):
    pass

class InvalidCursor(DatabaseError):
    pass

class ReadOnly(DatabaseError):
    pass

class ReplicationError(DatabaseError):
    pass

class ShardError(DatabaseError):
    """Base error for the embedded sharding layer."""
    pass

class MigrationInProgress(ShardError):
    """A topology change is open; another one cannot start until it drains."""
    pass

class CrossShardCommitError(ShardError):
    """A coordinated multi-shard commit could not finish across every shard.

    The coordinator ledger records which shards committed; the same request key
    resumes the remaining shards instead of leaving the outcome unreported.
    """
    def __init__(self, message, result=None):
        super().__init__(message)
        self.result = result

class UnknownShard(ShardError):
    pass
