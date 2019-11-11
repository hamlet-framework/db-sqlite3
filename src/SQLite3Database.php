<?php

namespace Hamlet\Database\SQLite3;

use Hamlet\Database\Database;
use Hamlet\Database\DatabaseException;
use Hamlet\Database\Session;
use Hamlet\Database\SimpleConnectionPool;
use SQLite3;

/**
 * @extends Database<SQLite3>
 */
class SQLite3Database extends Database
{
    public function __construct(string $location, int $flags = SQLITE3_OPEN_READWRITE | SQLITE3_OPEN_CREATE, string $encryptionKey = null)
    {
        parent::__construct(new SimpleConnectionPool(function () use ($location, $flags, $encryptionKey): SQLite3 {
            if ($encryptionKey) {
                return new SQLite3($location, $flags, $encryptionKey);
            } else {
                return new SQLite3($location, $flags);
            }
        }));
    }

    protected function createSession($handle): Session
    {
        $session = new SQLite3Session($handle);
        $session->setLogger($this->logger);
        return $session;
    }

    public static function exception(SQLite3 $connection): DatabaseException
    {
        throw new DatabaseException($connection->lastErrorMsg(), $connection->lastErrorCode());
    }
}
