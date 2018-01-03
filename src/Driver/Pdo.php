<?php

namespace Ions\Db\Driver;

/**
 * Class Pdo
 * @package Ions\Db\Driver
 */
class Pdo
{
    /**
     * @var array
     */
    protected $parameters;

    /**
     * @var
     */
    protected $dsn;

    /**
     * @var \PDO
     */
    protected $connection;

    /**
     * @var \PDOStatement
     */
    protected $statement;

    /**
     * @var
     */
    protected $result;

    /**
     * @var bool
     */
    protected $inTransaction = false;

    /**
     * @var string
     */
    protected $sql = '';

    /**
     * @var bool
     */
    protected $isPrepared = false;

    /**
     * Pdo constructor.
     * @param $connection
     * @throws \RuntimeException|\InvalidArgumentException
     */
    public function __construct($connection)
    {
        if (!extension_loaded('PDO')) {
            throw new \RuntimeException('The PDO extension is not loaded');
        }

        if (is_array($connection)) {
            $this->parameters = $connection;
        } elseif ($connection instanceof \PDO) {
            $this->connection = $connection;
        } elseif (null !== $connection) {
            throw new \InvalidArgumentException(
                '$connection must be an array of parameters, a PDO object or null'
            );
        }
    }

    /**
     * @return mixed
     * @throws \RuntimeException
     */
    public function getDsn()
    {
        if (!$this->dsn) {
            throw new \RuntimeException('The DSN has not been set or constructed from parameters in connect() for this Connection');
        }

        return $this->dsn;
    }

    /**
     * @return bool|mixed|string
     */
    public function getCurrentSchema()
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        switch ($this->parameters['driver']) {
            case 'pdo.mysql':
                $sql = 'SELECT DATABASE()';
                break;
            case 'pdo.sqlite':
                return 'main';
            case 'pdo.pgsql':
            default:
                $sql = 'SELECT CURRENT_SCHEMA';
                break;
        }

        $result = $this->connection->query($sql);

        if ($result instanceof \PDOStatement) {
            return $result->fetchColumn();
        }

        return false;
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
     * @return $this
     * @throws \InvalidArgumentException|\RuntimeException
     */
    public function connect()
    {
        if ($this->connection) {
            return $this;
        }

        $dsn = $username = $password = $hostname = $database = $driver = null;

        $options = [];

        foreach ($this->parameters as $key => $value) {
            switch (strtolower($key)) {
                case 'dsn':
                    $dsn = $value;
                    break;
                case 'driver':
                    $driver = substr($value, 4) ?: '';
                    break;
                case 'username':
                    $username = (string)$value;
                    break;
                case 'password':
                    $password = (string)$value;
                    break;
                case 'hostname':
                    $hostname = (string)$value;
                    break;
                case 'port':
                    $port = (int)$value;
                    break;
                case 'database':
                    $database = (string)$value;
                    break;
                case 'charset':
                    $charset = (string)$value;
                    break;
                case 'unix_socket':
                    $unix_socket = (string)$value;
                    break;
                case 'options':
                    $value = (array)$value;
                    $options = array_diff_key($options, $value) + $value;
                    break;
                default:
                    $options[$key] = $value;
                    break;
            }
        }

        if (isset($hostname, $unix_socket)) {
            throw new \InvalidArgumentException('Ambiguous connection parameters, both hostname and unix_socket parameters were set');
        }

        if (!isset($dsn) && isset($driver)) {

            $dsn = [];

            switch ($driver) {
                case 'sqlite':
                    $dsn[] = $database;
                    break;
                default:
                    if (isset($database)) {
                        $dsn[] = "dbname={$database}";
                    }

                    if (isset($hostname)) {
                        $dsn[] = "host={$hostname}";
                    }

                    if (isset($port)) {
                        $dsn[] = "port={$port}";
                    }

                    if (isset($charset) && $driver !== 'pgsql') {
                        $dsn[] = "charset={$charset}";
                    }

                    if (isset($unix_socket)) {
                        $dsn[] = "unix_socket={$unix_socket}";
                    }
                    break;
            }

            $dsn = $driver . ':' . implode(';', $dsn);

        } elseif (!isset($dsn)) {
            throw new \InvalidArgumentException('A dsn was not provided or could not be constructed from your parameters');
        }

        $this->dsn = $dsn;

        try {
            $this->connection = new \PDO($dsn, $username, $password, $options);
            $this->connection->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

            if (isset($charset) && $driver === 'pgsql') {
                $this->connection->exec('SET NAMES ' . $this->connection->quote($charset));
            }
        } catch (\PDOException $e) {
            $code = $e->getCode();
            if (!is_int($code)) {
                $code = null;
            }

            throw new \RuntimeException('Connect Error: ' . $e->getMessage(), $code, $e);
        }

        return $this;
    }

    /**
     * @return bool
     */
    public function isConnected()
    {
        return ($this->connection instanceof \PDO);
    }

    /**
     * @return $this
     */
    public function beginTransaction()
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        $this->connection->beginTransaction();
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

        return $this;
    }

    /**
     * @return $this
     */
    public function disconnect()
    {
        if ($this->isConnected()) {
            $this->connection = null;
        }

        return $this;
    }

    /**
     * @return array
     */
    public function getParameters()
    {
        return $this->parameters;
    }

    /**
     * @return \PDO
     */
    public function getConnection()
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        return $this->connection;
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
    public function rollback()
    {
        if (!$this->isConnected()) {
            throw new \RuntimeException('Must be connected before you can rollback');
        }

        if (!$this->inTransaction()) {
            throw new \RuntimeException('Must call beginTransaction() before you can rollback');
        }

        $this->connection->rollBack();
        $this->inTransaction = false;

        return $this;
    }

    /**
     * @return \stdClass
     * @throws \InvalidArgumentException
     */
    public function result()
    {
        if (!$this->result instanceof \PDOStatement) {
            throw new \InvalidArgumentException($this->statement->error);
        }

        $data = [];

        while ($row = $this->result->fetch(\PDO::FETCH_ASSOC)) {
            $data[] = $row;
        }

        $obj = new \stdClass();
        $obj->count = $this->count();
        $obj->row = isset($data[0]) ? $data[0] : [];
        $obj->rows = $data;

        return $obj;
    }

    /**
     * @param $sql
     * @return bool
     * @throws \RuntimeException
     */
    public function query($sql)
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        $this->result = $this->connection->query($sql);

        if ($this->result === false) {
            $errorInfo = $this->connection->errorInfo();
            throw new \RuntimeException($errorInfo[2]);
        }

        return true;
    }

    /**
     * @param null $sql
     * @return $this
     * @throws \RuntimeException
     */
    public function prepare($sql = null)
    {
        if ($this->isPrepared) {
            throw new \RuntimeException('This statement has been prepared already');
        }

        if ($sql === null) {
            $sql = $this->sql;
        }

        if (!$this->isConnected()) {
            $this->connect();
        }

        $this->statement = $this->connection->prepare($sql);

        if ($this->statement === false) {
            $error = $this->connection->errorInfo();
            throw new \RuntimeException($error[2]);
        }

        $this->isPrepared = true;
        return $this;
    }

    /**
     * @return bool
     * @throws \RuntimeException
     */
    public function isPrepared()
    {
        return $this->isPrepared;
    }

    public function execute()
    {
        if (!$this->isPrepared) {
            $this->prepare();
        }

        try {
            $this->statement->execute();
        } catch (\PDOException $e) {
            throw new \RuntimeException('Statement could not be executed (' . implode(' - ', $this->connection->errorInfo()) . ')', null, $e);
        }

        $this->isPrepared = false;

        return true;
    }

    /**
     * @return int
     */
    public function count()
    {
        return (int)$this->result->rowCount();
    }

    /**
     * @return mixed
     */
    public function countField()
    {
        return $this->result->columnCount();
    }

    /**
     * @return mixed
     */
    public function countAffected()
    {
        return $this->result->rowCount();
    }

    /**
     * @return bool|string
     */
    public function lastId()
    {
        try {
            return $this->connection->lastInsertId();
        } catch (\Exception $e) {
            return false;
        }
    }

    /**
     * @param $string
     * @return string
     */
    public function escape($string)
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        return $this->connection->quote(addcslashes((string)$string, "\x00\n\r\\'\"\x1a") );
    }

    /**
     * @return void
     */
    public function __destruct()
    {
        $this->disconnect();
    }
}
