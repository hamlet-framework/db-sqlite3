<?php

namespace Hamlet\Database\SQLite3;

use Hamlet\Database\Procedure;
use Hamlet\Database\Session;
use SQLite3;

/**
 * @extends Session<SQLite3>
 */
class SQLite3Session extends Session
{
    /**
     * @param SQLite3 $handle
     */
    public function __construct(SQLite3 $handle)
    {
        parent::__construct($handle);
    }

    /**
     * @param string $query
     * @return Procedure
     */
    public function prepare(string $query): Procedure
    {
        $procedure = new SQLite3Procedure($this->handle, $query);
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
            throw SQLite3Database::exception($connection);
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
            throw SQLite3Database::exception($connection);
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
            throw SQLite3Database::exception($connection);
        }
    }
}
