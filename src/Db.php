<?php

namespace Ions\Db;

/**
 * Class Db
 * @package Ions\Db
 */
class Db
{
    /**
     * @var Driver\Pdo
     */
    protected $driver;

    /**
     * Db constructor.
     * @param array $driver
     * @throws \InvalidArgumentException
     */
    public function __construct(array $driver)
    {
        $name = strtolower($driver['driver']);

        switch ($name) {
            case 'mysqli':
                $this->driver = new Driver\Mysqli($driver);
                break;
            case 'pgsql':
                $this->driver = new Driver\Pgsql($driver);
                break;
            case 'pdo':
            default:
                if ($name === 'pdo' || strpos($name, 'pdo') === 0) {
                    $this->driver = new Driver\Pdo($driver);
                }
        }

        if (!$this->driver) {
            throw new \InvalidArgumentException('Driver expected');
        }
    }

    /**
     * @return Driver\Pdo
     * @throws \RuntimeException
     */
    public function getDriver()
    {
        if ($this->driver === null) {
            throw new \RuntimeException('Driver has not been set or configured.');
        }

        return $this->driver;
    }

    /**
     * @return bool|mixed|string
     */
    public function getCurrentSchema()
    {
        return $this->driver->getCurrentSchema();
    }

    /**
     * @param $sql
     * @return \stdClass
     */
    public function query($sql)
    {
        $this->driver->query($sql);
        return $this->driver->result();
    }

    /**
     * @param $sql
     * @return bool
     */
    public function execute($sql)
    {
        $this->driver->prepare($sql);
        return $this->driver->execute();
    }

    /**
     * @param $value
     * @return string
     */
    public function escape($value)
    {
        return $this->driver->escape($value);
    }

    /**
     * @return mixed
     */
    public function getAffectedRows()
    {
        return $this->driver->countAffected();
    }

    /**
     * @return mixed
     */
    public function getFieldCount()
    {
        return $this->driver->countField();
    }

    /**
     * @return bool|string
     */
    public function getLastId()
    {
        return $this->driver->lastId();
    }

    /**
     * @return bool
     */
    public function connected()
    {
        return $this->driver->isConnected();
    }
}
