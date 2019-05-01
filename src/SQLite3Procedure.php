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

    /**
     * @return int
     * @psalm-suppress MixedInferredReturnType
     * @psalm-suppress MixedReturnStatement
     */
    public function insert(): int
    {
        $procedure = function (SQLite3 $connection): int {
            $this->bindParameters($connection)->execute();
            return $connection->lastInsertRowID();
        };
        return ($this->executor)($procedure);
    }

    /**
     * @return void
     */
    public function execute()
    {
        $procedure =
            /**
             * @param SQLite3 $connection
             * @return void
             */
            function (SQLite3 $connection) {
                $this->bindParameters($connection)->execute();
            };

        ($this->executor)($procedure);
    }

    /**
     * @return int
     * @psalm-suppress MixedInferredReturnType
     * @psalm-suppress MixedReturnStatement
     */
    public function affectedRows(): int
    {
        $procedure = function (SQLite3 $connection): int {
            return $connection->changes();
        };

        return ($this->executor)($procedure);
    }

    /**
     * @return Generator
     * @psalm-return Generator<int,array<string,int|string|float|null>,mixed,void>
     * @psalm-suppress MixedInferredReturnType
     * @psalm-suppress MixedReturnStatement
     */
    protected function fetch(): Generator
    {
        $procedure =
            /**
             * @param SQLite3 $connection
             * @return Generator
             * @psalm-return Generator<int,array<string,int|string|float|null>,mixed,void>
             * @psalm-suppress MixedReturnTypeCoercion
             */
            function (SQLite3 $connection) {
                $result = $this->bindParameters($connection)->execute();
                $index = 0;
                while (($row = $result->fetchArray(SQLITE3_ASSOC)) !== false) {
                    yield $index++ => $row;
                }
            };

        return ($this->executor)($procedure);
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
