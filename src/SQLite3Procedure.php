<?php

namespace Hamlet\Database\SQLite3;

use Generator;
use Hamlet\Database\DatabaseException;
use Hamlet\Database\Procedure;
use Hamlet\Database\Traits\QueryExpanderTrait;
use SQLite3;
use SQLite3Stmt;

class SQLite3Procedure extends Procedure
{
    use QueryExpanderTrait;

    /** @var callable */
    private $executor;

    /** @var string */
    private $query;

    public function __construct(callable $executor, string $query)
    {
        $this->executor = $executor;
        $this->query = $query;
    }

    public function insert(): int
    {
        return ($this->executor)(function (SQLite3 $connection) {
            $this->bindParameters($connection)->execute();
            return $connection->lastInsertRowID();
        });
    }

    public function execute(): void
    {
        ($this->executor)(function (SQLite3 $connection) {
            $this->bindParameters($connection)->execute();
        });
    }

    public function affectedRows(): int
    {
        return ($this->executor)(function (SQLite3 $connection): int {
            return $connection->changes();
        });
    }

    /**
     * @return Generator<int,array<string,int|string|float|null>>
     */
    protected function fetch(): Generator
    {
        return ($this->executor)(function (SQLite3 $connection) {
            $result = $this->bindParameters($connection)->execute();
            $index = 0;
            while (($row = $result->fetchArray(SQLITE3_ASSOC)) !== false) {
                yield $index++ => $row;
            }
        });
    }

    private function bindParameters(SQLite3 $connection): SQLite3Stmt
    {
        list($query, $parameters) = $this->unwrapQueryAndParameters($this->query, $this->parameters);
        $this->parameters = [];

        $statement = $connection->prepare($query);
        if ($statement === false) {
            throw new DatabaseException('Cannot prepare statement ' . $query);
        }

        $position = 1;
        foreach ($parameters as list($type, $value)) {
            $statement->bindValue($position++, $value, $this->resolveType($type, $value));
        }

        return $statement;
    }

    /**
     * @param string $type
     * @param string|int|float|null $value
     * @return int
     */
    private function resolveType(string $type, $value): int
    {
        if ($value === null) {
            return SQLITE3_NULL;
        }
        switch ($type) {
            case 'b':
                return SQLITE3_BLOB;
            case 'd':
                return SQLITE3_FLOAT;
            case 'i':
                return SQLITE3_INTEGER;
            case 's':
                return SQLITE3_TEXT;
            default:
                throw new DatabaseException('Cannot resolve type "' . $type . '"');
        }
    }
}
