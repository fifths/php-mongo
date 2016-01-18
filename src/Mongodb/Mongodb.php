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
    public static $instance;
    public $user;
    public $pwd;
    public $host;
    public $port;
    public $dbname;
    public $connect = array();

    public $mongo;
    public $db;
    public $error;

    public function __construct()
    {
        $this->connection_string();
    }

    public function connect()
    {
        try {
            $this->mongo = new \MongoClient("mongodb://{$this->user}:{$this->pwd}@{$this->host}:{$this->port}/{$this->dbname}", $this->connect);
            $this->db = $this->mongo->selectDB($this->dbname);
            return $this;
        } catch (\MongoConnectionException $e) {
            $this->error = $e->getMessage();
            return false;
        }
    }

    public  function connection_string()
    {
        $config = require('config.php');
        $this->user = isset($config['user'])?$config['user']:'';
        $this->pwd = isset($config['pwd'])?$config['pwd']:'';
        $this->host = isset($config['host'])?$config['host']:'';
        $this->port = isset($config['port'])?$config['port']:'';
        $this->dbname = isset($config['dbname'])?$config['dbname']:'';
        $this->connect = array(
            'connect' => isset($config['connect'])?$config['connect']:''
        );
    }


    public static function getInstance()
    {
        self::connect();
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
        if(is_resource($this->mongo)){
            $this->mongo->close();
        }
    }

}