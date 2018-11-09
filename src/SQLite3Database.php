<?php

namespace Hamlet\Database\SQLite3;

use Hamlet\Database\ConnectionPool;
use Hamlet\Database\Database;
use Hamlet\Database\DatabaseException;
use Hamlet\Database\Procedure;
use SQLite3;

/**
 * @template-extends Database<SQLite3>
 */
class SQLite3Database extends Database
{
    public function __construct(string $location, int $flags = SQLITE3_OPEN_READWRITE | SQLITE3_OPEN_CREATE, string $encryptionKey = null)
    {
        $connector = function () use ($location, $flags, $encryptionKey): SQLite3 {
            if ($encryptionKey) {
                return new SQLite3($location, $flags, $encryptionKey);
            } else {
                return new SQLite3($location, $flags);
            }
        };
        $pool = new ConnectionPool($connector);
        parent::__construct($pool);
    }

    public function prepare(string $query): Procedure
    {
        $procedure = new SQLite3Procedure($this->executor(), $query);
        $procedure->setLogger($this->logger);
        return $procedure;
    }

    /**
     * @param SQLite3 $connection
     * @return void
     */
    protected function startTransaction($connection)
    {
        $this->logger->debug('Starting transaction');
        $success = $connection->exec('BEGIN TRANSACTION');
        if (!$success) {
            throw self::exception($connection);
        }
    }

    /**
     * @param SQLite3 $connection
     * @return void
     */
    protected function commit($connection)
    {
        $this->logger->debug('Committing transaction');
        $success = $connection->exec('COMMIT');
        if (!$success) {
            throw self::exception($connection);
        }
    }

    /**
     * @param SQLite3 $connection
     * @return void
     */
    protected function rollback($connection)
    {
        $this->logger->debug('Rolling back transaction');
        $success = $connection->exec('ROLLBACK');
        if (!$success) {
            throw self::exception($connection);
        }
    }

    public static function exception(SQLite3 $connection): DatabaseException
    {
        throw new DatabaseException($connection->lastErrorMsg(), $connection->lastErrorCode());
    }
}
