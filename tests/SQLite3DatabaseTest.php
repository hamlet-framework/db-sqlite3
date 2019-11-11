<?php

namespace Hamlet\Database\SQLite3;

use Hamlet\Database\Database;
use Hamlet\Database\Procedure;
use Hamlet\Database\Session;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class SQLite3DatabaseTest extends TestCase
{
    /** @var Database */
    private $database;

    /** @var Procedure */
    private $procedure;

    public function setUp()
    {
        $this->database = new SQLite3Database(tempnam(sys_get_temp_dir(), '.sqlite'));
        $this->database->withSession(function (Session $session) {
            $session->prepare('
                CREATE TABLE users (
                  id INTEGER PRIMARY KEY,
                  name VARCHAR(255)
                )
            ')->execute();
            $session->prepare('
                CREATE TABLE addresses (
                  user_id INTEGER,
                  address VARCHAR(255)
                ) 
            ')->execute();

            $procedure = $session->prepare("INSERT INTO users (name) VALUES ('Vladimir')");
            $userId = $procedure->insert();

            $procedure = $session->prepare("INSERT INTO addresses (user_id, address) VALUES (?, 'Moskva')");
            $procedure->bindInteger($userId);
            $procedure->execute();

            $procedure = $session->prepare("INSERT INTO addresses (user_id, address) VALUES (?, 'Vladivostok')");
            $procedure->bindInteger($userId);
            $procedure->execute();

            $this->procedure = $session->prepare('
                SELECT users.id,
                       name,
                       address
                  FROM users 
                       JOIN addresses
                         ON users.id = addresses.user_id      
            ');
        });
    }

    public function testProcessOne()
    {
        $result = $this->procedure->processOne()
            ->coalesceAll()
            ->collectAll();

        Assert::assertEquals([1], $result);
    }

    public function testProcessAll()
    {
        $result = $this->procedure->processAll()
            ->selectValue('address')->groupInto('addresses')
            ->selectFields('name', 'addresses')->name('user')
            ->map('id', 'user')->flatten()
            ->collectAll();

        Assert::assertCount(1, $result);
        Assert::assertArrayHasKey(1, $result);
        Assert::assertEquals('Vladimir', $result[1]['name']);
        Assert::assertCount(2, $result[1]['addresses']);
    }

    public function testFetchOne()
    {
        Assert::assertEquals(['id' => '1', 'name' => 'Vladimir', 'address' => 'Moskva'], $this->procedure->fetchOne());
    }

    public function testFetchAll()
    {
        Assert::assertEquals([
            ['id' => '1', 'name' => 'Vladimir', 'address' => 'Moskva'],
            ['id' => '1', 'name' => 'Vladimir', 'address' => 'Vladivostok']
        ], $this->procedure->fetchAll());
    }

    public function testStream()
    {
        $iterator = $this->procedure->stream()
            ->selectValue('address')->groupInto('addresses')
            ->selectFields('name', 'addresses')->name('user')
            ->map('id', 'user')->flatten()
            ->iterator();

        foreach ($iterator as $id => $user) {
            Assert::assertEquals(1, $id);
            Assert::assertCount(2, $user['addresses']);
        }
    }

    public function testInsert()
    {
        $this->database->withSession(function (Session $session) {
            $procedure = $session->prepare("INSERT INTO users (name) VALUES ('Anatoly')");
            Assert::assertEquals(2, $procedure->insert());
        });
    }
}
