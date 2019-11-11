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

    /**
     * @var SQLite3
     */
    private $handle;

    /**
     * @var string
     */
    private $query;

    /**
     * @var SQLite3Stmt[]
     * @psalm-var array<string,SQLite3Stmt>
     */
    private $cache = [];

    public function __construct(SQLite3 $handle, string $query)
    {
        $this->handle = $handle;
        $this->query = $query;
    }

    /**
     * @return int
     * @psalm-suppress MixedInferredReturnType
     * @psalm-suppress MixedReturnStatement
     */
    public function insert(): int
    {
        $this->bindParameters($this->handle)->execute();
        return $this->handle->lastInsertRowID();
    }

    /**
     * @return void
     */
    public function execute()
    {
        $this->bindParameters($this->handle)->execute();
    }

    /**
     * @return int
     * @psalm-suppress MixedInferredReturnType
     * @psalm-suppress MixedReturnStatement
     */
    public function affectedRows(): int
    {
        return $this->handle->changes();
    }

    /**
     * @return Generator
     * @psalm-return Generator<int,array<string,int|string|float|null>,mixed,void>
     * @psalm-suppress MixedInferredReturnType
     * @psalm-suppress MixedReturnStatement
     * @psalm-suppress MixedReturnTypeCoercion
     */
    protected function fetch(): Generator
    {
        $result = $this->bindParameters($this->handle)->execute();
        $index = 0;
        while (($row = $result->fetchArray(SQLITE3_ASSOC)) !== false) {
            yield $index++ => $row;
        }
        $result->finalize();
    }

    private function bindParameters(SQLite3 $connection): SQLite3Stmt
    {
        list($query, $parameters) = $this->unwrapQueryAndParameters($this->query, $this->parameters);
        $this->parameters = [];

        $statement = $this->cache[$query] = ($this->cache[$query] ?? $connection->prepare($query));
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
