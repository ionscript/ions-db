<?php

namespace Ions\Db\Driver;

/**
 * Class Pgsql
 * @package Ions\Db\Driver
 */
class Pgsql
{
    /**
     * @var resource
     */
    protected $connection;

    /**
     * @var
     */
    protected $type;

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
     * @var int
     */
    protected static $stmtindex = 0;

    /**
     * @var
     */
    protected $stmtname;

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
     * Pgsql constructor.
     * @param $connection
     * @throws \RuntimeException
     */
    public function __construct($connection)
    {
        if (!extension_loaded('pgsql')) {
            throw new \RuntimeException('The PostgreSQL (pgsql) extension is not loaded');
        }

        if (is_array($connection)) {
            $this->parameters = $connection;
        } elseif (is_resource($connection)) {
            $this->connection = $connection;
        }
    }

    /**
     * @param $type
     * @return $this
     * @throws \InvalidArgumentException
     */
    public function setType($type)
    {
        $valid = ($type === PGSQL_CONNECT_FORCE_NEW);

        if (!$valid && defined('PGSQL_CONNECT_ASYNC')) {
            $valid = ($type === PGSQL_CONNECT_ASYNC);
        }

        if (!$valid) {
            throw new \InvalidArgumentException('Connection type is not valid.');
        }

        $this->type = $type;

        return $this;
    }

    /**
     * @return null
     */
    public function getCurrentSchema()
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        $result = pg_query($this->connection, 'SELECT CURRENT_SCHEMA AS "currentschema"');

        if ($result === false) {
            return null;
        }

        return pg_fetch_result($result, 0, 'currentschema');
    }

    /**
     * @return $this
     * @throws \RuntimeException
     */
    public function connect()
    {
        if (is_resource($this->connection)) {
            return $this;
        }

        $parameters = [
            'host' => $this->parameters['hostname'],
            'user' => $this->parameters['username'],
            'password' => $this->parameters['password'],
            'dbname' => $this->parameters['database'],
            'port' => isset($this->parameters['port']) ? (int)$this->parameters['port'] : null,
            'socket' => isset($this->parameters['socket']) ? $this->parameters['socket'] : null
        ];

        $this->connection = pg_connect(urldecode(http_build_query(array_filter($parameters), null, ' ')));

        if ($this->connection === false) {
            throw new \RuntimeException(sprintf('%s: Unable to connect to database', __METHOD__));
        }

        if (!empty($this->parameters['charset'])) {
            if (-1 === pg_set_client_encoding($this->connection, $this->parameters['charset'])) {
                throw new \RuntimeException(sprintf(
                    "%s: Unable to set client encoding '%s'",
                    __METHOD__,
                    $this->parameters['charset']
                ));
            }
        }

        return $this;
    }

    /**
     * @return bool
     */
    public function isConnected()
    {
        return is_resource($this->connection);
    }

    /**
     * @return $this
     */
    public function disconnect()
    {
        pg_close($this->connection);
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
    public function inTransaction()
    {
        return $this->inTransaction;
    }

    /**
     * @return $this
     * @throws \RuntimeException
     */
    public function beginTransaction()
    {
        if ($this->inTransaction()) {
            throw new \RuntimeException('Nested transactions are not supported');
        }

        if (!$this->isConnected()) {
            $this->connect();
        }

        pg_query($this->connection, 'BEGIN');

        $this->inTransaction = true;

        return $this;
    }

    /**
     * @return $this|bool
     */
    public function commit()
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        if (!$this->inTransaction()) {
            return false;
        }

        pg_query($this->connection, 'COMMIT');

        $this->inTransaction = false;

        return $this;
    }

    /**
     * @return $this
     * @throws \RuntimeException
     */
    public function rollback()
    {
        if (!$this->isConnected()) {
            throw new \RuntimeException('Must be connected before you can rollback');
        }

        if (!$this->inTransaction()) {
            throw new \RuntimeException('Must call beginTransaction() before you can rollback');
        }

        pg_query($this->connection, 'ROLLBACK');

        $this->inTransaction = false;

        return $this;
    }

    /**
     * @param $sql
     * @return bool
     * @throws \InvalidArgumentException
     */
    public function query($sql)
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        $this->result = pg_query($this->connection, $sql);

        if ($this->result === false) {
            throw new \InvalidArgumentException(pg_errormessage());
        }

        return true;
    }

    /**
     * @param null $sql
     */
    public function prepare($sql = null)
    {
        $sql = $sql ?: $this->sql;

        $count = 1;

        $sql = preg_replace_callback('#\$\##', function () use (&$count) {
            return '$' . $count++;
        }, $sql);

        $this->sql = $sql;
        $this->stmtname = 'statement' . ++static::$stmtindex;
        $this->statement = pg_prepare($this->connection, $this->stmtname, $sql);
    }

    /**
     * @return bool
     */
    public function isPrepared()
    {
        return isset($this->resource);
    }

    /**
     * @param null $parameters
     * @return bool
     * @throws \InvalidArgumentException
     */
    public function execute($parameters = null)
    {
        if (!$this->isPrepared()) {
            $this->prepare();
        }

        $this->result = pg_execute($this->connection, $this->stmtname, (array)$parameters);

        if ($this->result=== false) {
            throw new \InvalidArgumentException(pg_last_error());
        }

        return true;
    }

    /**
     * @return \stdClass
     * @throws \InvalidArgumentException
     */
    public function result()
    {
        if (!is_resource($this->result)) {
            throw new \InvalidArgumentException(pg_last_error());
        }

        $data = [];

        while ($row = pg_fetch_assoc($this->result)) {
            $data[] = $row;
        }

        $obj = new \stdClass();
        $obj->count = $this->count();
        $obj->row = isset($data[0]) ? $data[0] : [];
        $obj->rows = $data;

        return $obj;
    }

    /**
     * @param null $name
     * @return null
     */
    public function lastId($name = null)
    {
        if ($name === null) {
            return null;
        }

        $result = pg_query($this->connection, 'SELECT CURRVAL(\'' . str_replace('\'', '\\\'', $name) . '\') as "currval"');

        return pg_fetch_result($result, 0, 'currval');
    }

    /**
     * @return mixed
     */
    public function countAffected()
    {
        return pg_affected_rows($this->result);
    }

    /**
     * @return mixed
     */
    public function count()
    {
        return pg_num_rows($this->result);
    }

    /**
     * @return mixed
     */
    public function countField()
    {
        return pg_num_fields($this->result);
    }

    /**
     * @param $string
     * @return string
     */
    public function escape($string)
    {
        return '\'' . pg_escape_string($this->connection, $string) . '\'';
    }

    /**
     * @return void
     */
    public function __destruct() {
        $this->disconnect();
    }
}
