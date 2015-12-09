<?php

namespace Mongodb;

/**
 * User: lee
 * Date: 15-10-26
 * Time: 4:02 pm
 */

/**
 *
 * Class Mongodb
 * @package Mongodb
 *
 * http://php.net/manual/zh/mongoclient.construct.php
 */
class Mongodb
{
    private static $instance;
    private $user;
    private $pwd;
    private $host;
    private $port;
    private $dbname;
    private $connect=array();

    private $mongo;
    public $db;
    private $error;

    final private function __construct()
    {
        $this->connection_string();
        $this->connect();
    }

    private function connect()
    {
        try {
            $this->mongo = new \MongoClient("mongodb://{$this->user}:{$this->pwd}@{$this->host}:{$this->port}/{$this->dbname}",$this->connect);
            $this->db = $this->mongo->selectDB($this->dbname);
            return $this;
        } catch (\MongoConnectionException $e) {
            $this->error = $e->getMessage();
            return false;
        }
    }

    private function connection_string()
    {
        $config = require('config.bak.php');
        $this->user = $config['user'];
        $this->pwd = $config['pwd'];
        $this->host = $config['host'];
        $this->port = $config['port'];
        $this->dbname = $config['dbname'];
        $this->connect = array(
            'connect'=>$config['connect']
        );
    }

    public static function getInstance()
    {
        if (!(self::$instance instanceof self)) {
            self::$instance = new self;
        }
        return self::$instance;
    }

    public function __clone()
    {
        trigger_error('Clone is not allow!', E_USER_ERROR);
    }

    public function __destruct()
    {
        $this->mongo->close();
    }

}