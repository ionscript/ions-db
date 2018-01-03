<?php

namespace Ions\Db\Driver;

/**
 * Class Mysqli
 * @package Ions\Db\Driver
 */
class Mysqli
{
    /**
     * @var \mysqli
     */
    protected $connection;

    /**
     * @var
     */
    protected $statement;

    /**
     * @var
     */
    protected $result;

    /**
     * @var array
     */
    protected $parameters = [];

    /**
     * @var string
     */
    protected $sql = '';

    /**
     * @var bool
     */
    protected $isPrepared = false;

    /**
     * @var bool
     */
    protected $inTransaction = false;

    /**
     * Mysqli constructor.
     * @param $connection
     * @throws \RuntimeException|\InvalidArgumentException
     */
    public function __construct($connection)
    {
        if (!extension_loaded('mysqli')) {
            throw new \RuntimeException('The Mysqli extension is not loaded');
        }

        if (is_array($connection)) {
            $this->parameters = $connection;
        } elseif ($connection instanceof \mysqli) {
            $this->connection = $connection;
        } elseif (!$connection) {
            throw new \InvalidArgumentException('$connection must be an array of parameters, a mysqli object or null');
        }
    }

    /**
     * @return mixed
     */
    public function getCurrentSchema()
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        $result = $this->connection->query('SELECT DATABASE()');
        $r = $result->fetch_row();

        return $r[0];
    }

    /**
     * @return $this
     * @throws \RuntimeException|\ErrorException
     */
    public function connect()
    {
        if ($this->connection instanceof \mysqli) {
            return $this;
        }

        $parameters = $this->parameters;
        $this->connection = new \mysqli();
        $this->connection->init();
        //TODO: check and set SQL_MODE
        //$this->connection->query("SET SQL_MODE = ''");

        if (!empty($parameters['options'])) {
            foreach ((array)$parameters['options'] as $option => $value) {
                $this->connection->options($option, $value);
            }
        }

        $this->connection->real_connect(
            $parameters['hostname'],
            $parameters['username'],
            $parameters['password'],
            $parameters['database'],
            isset($parameters['port']) ? (int)$parameters['port'] : null,
            isset($parameters['socket']) ? $parameters['socket'] : null
        );

        if ($this->connection->connect_error) {
            throw new \RuntimeException(
                'Connection error',
                null,
                new \ErrorException(
                    $this->connection->connect_error, $this->connection->connect_errno
                )
            );
        }

        if (!empty($parameters['charset'])) {
            $this->connection->set_charset($parameters['charset']);
        }

        return $this;
    }

    /**
     * @return bool
     */
    public function isConnected()
    {
        return $this->connection instanceof \mysqli;
    }

    /**
     * @return void
     */
    public function disconnect()
    {
        if ($this->connection instanceof \mysqli) {
            $this->connection->close();
        }

        $this->connection = null;
    }

    /**
     * @return bool
     */
    public function inTransaction()
    {
        return $this->inTransaction;
    }

    /**
     * @return $this
     */
    public function beginTransaction()
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        $this->connection->autocommit(false);
        $this->inTransaction = true;

        return $this;
    }

    /**
     * @return $this
     */
    public function commit()
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        $this->connection->commit();
        $this->inTransaction = false;
        $this->connection->autocommit(true);

        return $this;
    }

    /**
     * @return $this
     * @throws \RuntimeException
     */
    public function rollback()
    {
        if (!$this->isConnected()) {
            throw new \RuntimeException('Must be connected before you can rollback.');
        }

        if (!$this->inTransaction) {
            throw new \RuntimeException('Must call beginTransaction() before you can rollback.');
        }

        $this->connection->rollback();
        $this->connection->autocommit(true);
        $this->inTransaction = false;

        return $this;
    }

    /**
     * @param $sql
     * @return $this
     */
    public function setSql($sql)
    {
        $this->sql = $sql;
        return $this;
    }

    /**
     * @return string
     */
    public function getSql()
    {
        return $this->sql;
    }

    /**
     * @return bool
     */
    public function isPrepared()
    {
        return $this->isPrepared;
    }

    /**
     * @param $sql
     * @return bool
     */
    public function query($sql)
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        $this->result = $this->connection->query($sql);

        return true;
    }

    /**
     * @return \stdClass
     * @throws \InvalidArgumentException
     */
    public function result()
    {
        if (!$this->result instanceof \mysqli_result) {
            throw new \InvalidArgumentException($this->connection->error);
        }

        $data = [];

        while ($row = $this->result->fetch_assoc()) {
            $data[] = $row;
        }

        $obj = new \stdClass();
        $obj->count = $this->count();
        $obj->row = isset($data[0]) ? $data[0] : [];
        $obj->rows = $data;

        return $obj;
    }

    /**
     * @param string $sql
     * @return $this
     * @throws \RuntimeException|\InvalidArgumentException|\ErrorException
     */
    public function prepare($sql = '')
    {
        if ($this->isPrepared) {
            throw new \RuntimeException('This statement has already been prepared');
        }

        if ($sql === null) {
            $sql = $this->sql;
        }

        if (!$this->isConnected()) {
            $this->connect();
        }

        $this->statement = $this->connection->prepare($sql);

        if (!$this->statement instanceof \mysqli_stmt) {
            throw new \InvalidArgumentException(
                'Statement couldn\'t be produced with sql: ' . $sql,
                null,
                new \ErrorException($this->connection->error, $this->connection->errno)
            );
        }

        $this->isPrepared = true;

        return $this;
    }

    /**
     * @return bool
     * @throws \RuntimeException
     */
    public function execute()
    {
        if (!$this->isPrepared) {
            $this->prepare();
        }

        $this->result = $this->statement->execute();

        if ($this->result === false) {
            throw new \RuntimeException($this->statement->error);
        }

        $this->isPrepared = false;

        return true;
    }

    /**
     * @return mixed
     */
    public function lastId()
    {
        return $this->connection->insert_id;
    }

    /**
     * @param $value
     * @return string
     */
    public function escape($value)
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        return '\'' . $this->connection->real_escape_string($value) . '\'';
    }

    /**
     * @return int
     */
    public function count() {
        if ($this->result instanceof \mysqli_result) {
            return $this->result->num_rows;
        }

        return 0;
    }

    /**
     * @return mixed
     */
    public function countField()
    {
        return $this->connection->field_count;
    }

    /**
     * @return int
     */
    public function countAffected() {
        if ($this->connection instanceof \mysqli || $this->connection instanceof \mysqli_stmt) {
            return $this->connection->affected_rows;
        }

        return 0;
    }

    /**
     * @return mixed
     */
    public function ping() {
        return $this->connection->ping();
    }

    /**
     * @return void
     */
    public function __destruct() {
        $this->disconnect();
    }
}
