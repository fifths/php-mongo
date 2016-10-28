<?php

namespace Fifths;

class Mongo_db
{
    private $config;
    private $hostname;
    private $port = 27017;
    private $database;
    private $username;
    private $password;
    private $debug = false;

    private $collection = '';
    private $selects;
    private $wheres;
    private $updates;
    private $limit = 999999;
    private $offset = 0;
    private $sorts;

    private $manager;
    private $result;

    public function __construct($config)
    {
        $this->config = $config;
        $this->connect();
    }

    /**
     * 预处理
     */
    private function prepareConfig()
    {
        if (isset($this->config['hostname'])) {
            $this->hostname = trim($this->config['hostname']);
        }
        if (isset($this->config['port'])) {
            $this->port = trim($this->config['port']);
        }
        if (isset($this->config['username'])) {
            $this->username = trim($this->config['username']);
        }
        if (isset($this->config['password'])) {
            $this->password = trim($this->config['password']);
        }
        if (isset($this->config['database'])) {
            $this->database = trim($this->config['database']);
        }
        if (isset($this->config['db_debug'])) {
            $this->debug = $this->config['db_debug'];
        }
    }

    /**
     * 链接
     */
    private function connect()
    {
        $this->prepareConfig();
        try {
            $dsn = "mongodb://{$this->hostname}:{$this->port}/{$this->database}";
            $options = array(
                'username' => $this->username,
                'password' => $this->password
            );
            $this->manager = new \MongoDB\Driver\Manager($dsn, $options);
        } catch (\Exception $e) {
            $this->showError($e);
        }
    }

    /**
     * 获取当前连接
     * @return mixed
     */
    public function getManager()
    {
        return $this->manager;
    }


    /**
     * @param mixed $collection
     */
    public function getCollection(): string
    {
        return $this->collection;
    }

    /**
     * 获取查询所需胡字段
     * @return array
     */
    public function getSelects(): array
    {
        return $this->selects;
    }

    /**
     * 获取条件
     * @return array
     */
    public function getWheres(): array
    {
        return $this->wheres;
    }

    /**
     * 获取更新内容
     * @return array
     */
    public function getUpdates(): array
    {
        return $this->updates;
    }

    /**
     * 获取条数
     * @return int
     */
    public function getLimit(): int
    {
        return $this->limit;
    }

    /**
     * 获取偏移量
     * @return int
     */
    public function getOffset(): int
    {
        return $this->offset;
    }

    /**
     * 获取排序
     * @return array
     */
    public function getSorts(): array
    {
        return $this->sorts;
    }

    /**
     * @param mixed $collection
     */
    public function setCollection(string $collection)
    {
        $this->collection = $collection;
    }

    /**
     * @param array $selects
     */
    public function setSelects(array $selects)
    {
        $this->selects = $selects;
    }

    /**
     * @param array $wheres
     */
    public function setWheres(array $wheres)
    {
        $this->wheres = $wheres;
    }

    /**
     * @param array $updates
     */
    public function setUpdates(array $updates)
    {
        $this->updates = $updates;
    }

    /**
     * @param int $limit
     */
    public function setLimit(int $limit)
    {
        $this->limit = $limit;
    }

    /**
     * @param int $offset
     */
    public function setOffset(int $offset)
    {
        $this->offset = $offset;
    }

    /**
     * @param array $sorts
     */
    public function setSorts(array $sorts)
    {
        $this->sorts = $sorts;
    }


    /**
     * @param $database
     * @return $this
     */
    public function switch_db($database)
    {
        $this->database = $database;
        return $this;
    }

    /**
     * @param $table
     * @return $this
     */
    public function collection($collection)
    {
        $this->collection = $collection;
        return $this;
    }

    /**
     * @param $collection
     * @return Mongo_db
     */
    public function table($collection)
    {
        return $this->collection($collection);
    }

    /**
     * 增
     * @param array $document
     * @param string $wstring
     * @param int $wtimeout
     * @return mixed
     */
    public function insert(
        $document = array(),
        $wstring = \MongoDB\Driver\WriteConcern::MAJORITY,
        $wtimeout = 1000)
    {
        try {
            $wc = new \MongoDB\Driver\WriteConcern($wstring, $wtimeout);
            $bulk = new \MongoDB\Driver\BulkWrite();
            $bulk->insert($document);
            $dbc = $this->database . '.' . $this->collection;
            $result = $this->manager->executeBulkWrite($dbc, $bulk, $wc);
            $this->result = $result;
            //增加几条
            return $result->getInsertedCount();
        } catch (\Exception $e) {
            $this->showError($e);
        }
    }

    /**
     * 批量添加
     * @param array $documents
     * @param string $wstring
     * @param int $wtimeout
     * @return mixed
     */
    public function batch_insert(
        $documents = array(),
        $wstring = \MongoDB\Driver\WriteConcern::MAJORITY,
        $wtimeout = 1000)
    {
        try {
            $wc = new \MongoDB\Driver\WriteConcern($wstring, $wtimeout);
            $bulk = new \MongoDB\Driver\BulkWrite();
            foreach ($documents as $k => $document) {
                $bulk->insert($document);
            }
            $dbc = $this->database . '.' . $this->collection;
            $result = $this->manager->executeBulkWrite($dbc, $bulk, $wc);
            $this->result = $result;
            //增加几条
            return $result->getInsertedCount();
        } catch (\Exception $e) {
            $this->showError($e);
        }
    }

    /**
     * 删
     * @param array $deleteOptions
     * @param string $wstring
     * @param int $wtimeout
     * @return mixed
     */
    public function delete(
        $deleteOptions = ["limit" => 1],
        $wstring = \MongoDB\Driver\WriteConcern::MAJORITY,
        $wtimeout = 1000
    )
    {
        try {
            $wc = new \MongoDB\Driver\WriteConcern($wstring, $wtimeout);
            $bulk = new \MongoDB\Driver\BulkWrite();
            $filter = $this->wheres;
            if (count($filter) < 1 && $deleteOptions['limit'] == 1) {
                throw new \Exception('filter is error!');
            }
            $bulk->delete($filter, $deleteOptions);
            $dbc = $this->database . '.' . $this->collection;
            $result = $this->manager->executeBulkWrite($dbc, $bulk, $wc);
            $this->result = $result;
            //删除几条
            return $result->getDeletedCount();
        } catch
        (\Exception $e) {
            $this->showError($e);
        }
    }


    /**
     * 删除所有
     * @param array $deleteOptions
     * @param string $wstring
     * @param int $wtimeout
     * @return mixed
     */
    public function delete_all(
        $deleteOptions = ["limit" => 0],
        $wstring = \MongoDB\Driver\WriteConcern::MAJORITY,
        $wtimeout = 1000
    )
    {
        return $this->delete($deleteOptions, $wstring, $wtimeout);
    }

    /**
     * 更新
     * @param array $updateOptions
     * @param string $wstring
     * @param int $wtimeout
     */
    public function update(
        $updateOptions = ['multi' => false, 'upsert' => false],
        $wstring = \MongoDB\Driver\WriteConcern::MAJORITY,
        $wtimeout = 1000
    )
    {
        try {
            $wc = new \MongoDB\Driver\WriteConcern($wstring, $wtimeout);
            $bulk = new \MongoDB\Driver\BulkWrite();
            $filter = $this->wheres;
            if (count($filter) < 1 && $updateOptions['multi'] == false) {
                throw new \Exception('filter is error!');
            }
            $newObj = $this->updates;
            $bulk->update(
                $filter,
                $newObj,
                $updateOptions
            );
            $dbc = $this->database . '.' . $this->collection;
            $result = $this->manager->executeBulkWrite($dbc, $bulk, $wc);
            $this->result = $result;
            return $result->getModifiedCount();
        } catch (\Exception $e) {
            $this->showError($e);
        }
    }

    /**
     * 更新所有
     * @param array $updateOptions
     * @param string $wstring
     * @param int $wtimeout
     */
    public function update_all(
        $updateOptions = ['multi' => true, 'upsert' => false],
        $wstring = \MongoDB\Driver\WriteConcern::MAJORITY,
        $wtimeout = 1000
    )
    {
        return $this->update($updateOptions, $wstring, $wtimeout);
    }

    /**
     * 查询单条
     * @param null $id
     * @return mixed|null
     */
    public function find($id = null)
    {
        if ($id != null) {
            $this->where('_id', new \MongoDB\BSON\ObjectID($id));
        }
        $filter = $this->wheres;
        $options = [
            'projection' => $this->selects,
            "sort" => $this->sorts,
            "skip" => 0,
            "limit" => 1,
        ];
        $query = new \MongoDB\Driver\Query($filter, $options);
        $dbc = $this->database . '.' . $this->collection;
        $documents = $this->manager->executeQuery($dbc, $query);
        $this->result = $documents;
        $returns = null;
        foreach ($documents as $document) {
            $bson = \MongoDB\BSON\fromPHP($document);
            $returns = json_decode(\MongoDB\BSON\toJSON($bson), true);
        }
        return $returns;
    }

    /**
     * command
     * @param $db
     * @param $commands
     * @return mixed
     */
    public function command($db, $commands)
    {
        try {
            $cursor = $this->manager->executeCommand($db, $commands);
            $this->result = $cursor;
            return $cursor;
        } catch (\Exception $e) {
            $this->showError($e);
        }
    }

    public function dropDatabase()
    {
        $cmd = array(
            'dropDatabase' => 1,
        );
        $db = $this->database;
        $commands = new \MongoDB\Driver\Command($cmd);
        $cursor = $this->command($db, $commands);
        $this->result = $cursor;
        $response = current($cursor->toArray());
        return $response;
    }

    public function drop_collection()
    {
        $cmd = array(
            'drop' => $this->collection,
        );
        $db = $this->database;
        $commands = new \MongoDB\Driver\Command($cmd);
        $cursor = $this->command($db, $commands);
        $this->result = $cursor;
        $response = current($cursor->toArray());
        return $response;
    }

    //unique
    public function add_index($key, $name = 'index')
    {
        $cmd = array(
            'createIndexes' => $this->collection,
            'indexes' => array(
                array(
                    'name' => $name,
                    'key' => $key,
                )
            )
        );
        $db = $this->database;
        $commands = new \MongoDB\Driver\Command($cmd);
        $cursor = $this->command($db, $commands);
        $this->result = $cursor;
        $response = current($cursor->toArray());
        return $response;
    }

    public function remove_index($index)
    {
        $cmd = array(
            'dropIndexes' => $this->collection,
            'index' => $index
        );
        $db = $this->database;
        $commands = new \MongoDB\Driver\Command($cmd);
        $cursor = $this->command($db, $commands);
        $this->result = $cursor;
        $response = current($cursor->toArray());
        return $response;
    }

    public function list_indexes()
    {
        $cmd = array(
            'listIndexes' => $this->collection,
        );
        $db = $this->database;
        $commands = new \MongoDB\Driver\Command($cmd);
        $cursor = $this->command($db, $commands);
        $this->result = $cursor;
        return $cursor;
    }

    public function aggregate($commands)
    {
        $db = $this->database;
        $commands = new \MongoDB\Driver\Command(
            [
                'aggregate' => $this->collection,
                'pipeline' => [$commands]
            ]
        );
        $cursor = $this->command($db, $commands);
        $this->result = $cursor;
        $response = current($cursor->toArray())->result;
        return $response;
    }

    /**
     * @param $key
     * @return mixed
     */
    public function distinct($key)
    {
        $db = $this->database;
        $commands = new \MongoDB\Driver\Command(
            [
                'distinct' => $this->collection,
                'key' => $key,
                'query' => $this->wheres
            ]
        );
        $cursor = $this->command($db, $commands);
        $this->result = $cursor;
        $response = current($cursor->toArray())->values;
        return $response;
    }

    /**
     * count
     * @return mixed
     */
    public function count()
    {
        $db = $this->database;
        $commands = new \MongoDB\Driver\Command(
            [
                "count" => $this->collection,
                "query" => $this->wheres
            ]
        );
        $cursor = $this->command($db, $commands);
        $this->result = $cursor;
        $response = $cursor->toArray()[0];
        return $response->n;
    }

    /**
     * 查
     * @return mixed
     */
    public function get()
    {
        try {
            $filter = (array)$this->wheres;
            $options = [
                'projection' => (array)$this->selects,
                "sort" => (array)$this->sorts,
                "skip" => (int)$this->offset,
                "limit" => (int)$this->limit,
            ];
            $query = new \MongoDB\Driver\Query($filter, $options);
            $dbc = $this->database . '.' . $this->collection;
            $documents = $this->manager->executeQuery($dbc, $query);
            $this->result = $documents;
            $returns = array();
            foreach ($documents as $document) {
                $bson = \MongoDB\BSON\fromPHP($document);
                $returns[] = json_decode(\MongoDB\BSON\toJSON($bson), true);
            }
            return $returns;
        } catch (\Exception $e) {
            $this->showError($e);
        }
    }

    /**
     * @param $fields
     * @param null $value
     * @return $this
     */
    public function set($fields, $value = NULL)
    {
        if (is_string($fields)) {
            $this->updates['$set'][$fields] = $value;
        } elseif (is_array($fields)) {
            foreach ($fields as $field => $value) {
                $this->updates['$set'][$field] = $value;
            }
        }
        return $this;
    }

    /**
     * 要获取的字段
     * @param $wheres
     * @param null $value
     * @return $this
     */
    public function field($includes = array(), $excludes = array())
    {
        if (!is_array($includes)) {
            $includes = array();
        }
        if (!is_array($excludes)) {
            $excludes = array();
        }
        if (!empty($includes)) {
            foreach ($includes as $col) {
                $this->selects[$col] = 1;
            }
        }
        if (!empty($excludes)) {
            foreach ($excludes as $col) {
                $this->selects[$col] = 0;
            }
        }
        return $this;
    }

    /**
     * 条件
     * @param $wheres
     * @param null $value
     * @return $this
     */
    public function where($wheres, $value = null)
    {
        if (is_array($wheres)) {
            foreach ($wheres as $wh => $val) {
                $this->wheres[$wh] = $val;
            }
        } else {
            $this->wheres[$wheres] = $value;
        }
        return $this;
    }

    public function where_in($field = "", $in = array())
    {
        $this->wheres[$field]['$in'] = $in;
        return $this;
    }

    public function where_in_all($field = "", $in = array())
    {
        $this->wheres[$field]['$all'] = $in;
        return $this;
    }

    public function where_or($wheres = array())
    {
        foreach ($wheres as $wh => $val) {
            $this->wheres['$or'][] = array($wh => $val);
        }
        return $this;
    }


    public function where_not_in($field = "", $in = array())
    {
        $this->wheres[$field]['$nin'] = $in;
        return $this;
    }

    public function where_gt($field = "", $x)
    {
        $this->wheres[$field]['$gt'] = $x;
        return $this;
    }

    public function where_gte($field = "", $x)
    {
        $this->wheres[$field]['$gte'] = $x;
        return $this;
    }

    public function where_lt($field = "", $x)
    {
        $this->wheres[$field]['$lt'] = $x;
        return $this;
    }

    public function where_lte($field = "", $x)
    {
        $this->wheres[$field]['$lte'] = $x;
        return $this;
    }

    public function where_between($field = "", $x, $y)
    {
        $this->wheres[$field]['$gte'] = $x;
        $this->wheres[$field]['$lte'] = $y;
        return $this;
    }

    public function where_between_ne($field = "", $x, $y)
    {
        $this->wheres[$field]['$gt'] = $x;
        $this->wheres[$field]['$lt'] = $y;
        return $this;
    }

    public function where_ne($field = '', $x)
    {
        $this->wheres[$field]['$ne'] = $x;
        return $this;
    }

    public function push($fields, $value = array())
    {
        if (is_string($fields)) {
            $this->updates['$push'][$fields] = $value;
        } elseif (is_array($fields)) {
            foreach ($fields as $field => $value) {
                $this->updates['$push'][$field] = $value;
            }
        }
        return $this;
    }

    public function addtoset($field, $values)
    {
        if (is_string($values)) {
            $this->updates['$addToSet'][$field] = $values;
        } elseif (is_array($values)) {
            $this->updates['$addToSet'][$field] = array('$each' => $values);
        }
        return $this;
    }

    public function pop($field)
    {
        if (is_string($field)) {
            $this->updates['$pop'][$field] = -1;
        } elseif (is_array($field)) {
            foreach ($field as $pop_field) {
                $this->updates['$pop'][$pop_field] = -1;
            }
        }
        return $this;
    }

    public function pull($field = "", $value = array())
    {
        $this->updates['$pull'] = array($field => $value);
        return $this;
    }

    public function rename_field($old, $new)
    {
        $this->updates['$rename'] = array($old => $new);
        return $this;
    }

    public function unset_field($fields)
    {
        if (is_string($fields)) {
            $this->updates['$unset'][$fields] = 1;
        } elseif (is_array($fields)) {
            foreach ($fields as $field) {
                $this->updates['$unset'][$field] = 1;
            }
        }
        return $this;
    }

    public function inc($fields = array(), $value = 0)
    {
        if (is_string($fields)) {
            $this->updates['$inc'][$fields] = $value;
        } elseif (is_array($fields)) {
            foreach ($fields as $field => $value) {
                $this->updates['$inc'][$field] = $value;
            }
        }
        return $this;
    }

    public function mul($fields = array(), $value = 0)
    {
        if (is_string($fields)) {
            $this->updates['$mul'][$fields] = $value;
        } elseif (is_array($fields)) {
            foreach ($fields as $field => $value) {
                $this->updates['$mul'][$field] = $value;
            }
        }
        return $this;
    }

    public function max($fields = array(), $value = 0)
    {
        if (is_string($fields)) {
            $this->updates['$max'][$fields] = $value;
        } elseif (is_array($fields)) {
            foreach ($fields as $field => $value) {
                $this->updates['$max'][$field] = $value;
            }
        }
        return $this;
    }

    public function min($fields = array(), $value = 0)
    {
        if (is_string($fields)) {
            $this->updates['$min'][$fields] = $value;
        } elseif (is_array($fields)) {
            foreach ($fields as $field => $value) {
                $this->updates['$min'][$field] = $value;
            }
        }
        return $this;
    }

    /**
     * 排序
     * @param array $fields
     * @return $this
     */
    public function order_by($fields = array())
    {
        foreach ($fields as $col => $val) {
            if ($val == -1 || $val === FALSE || strtolower($val) == 'desc') {
                $this->sorts[$col] = -1;
            } else {
                $this->sorts[$col] = 1;
            }
        }
        return $this;
    }

    /**
     * 条数
     * @param int $x
     * @return $this
     */
    public function limit($x = 99999)
    {
        if ($x !== NULL && is_numeric($x) && $x >= 1) {
            $this->limit = (int)$x;
        }
        return $this;
    }

    /**
     * 偏移量
     * @param int $x
     * @return $this
     */
    public function offset($x = 0)
    {
        if ($x !== NULL && is_numeric($x) && $x >= 1) {
            $this->offset = (int)$x;
        }
        return $this;
    }

    /**
     * 生成mongo时间
     * @param bool $stamp
     * @return \MongoDB\BSON\UTCDatetime
     */
    public function date($stamp = false)
    {
        if ($stamp == false) {
            return new \MongoDB\BSON\UTCDatetime(time() * 1000);
        } else {
            return new \MongoDB\BSON\UTCDatetime($stamp);
        }

    }

    /**
     * 生成mongo时间戳
     * @param bool $stamp
     * @return \MongoDB\BSON\Timestamp
     */
    public function timestamp($stamp = false)
    {
        if ($stamp == false) {
            return new \MongoDB\BSON\Timestamp(0, time());
        } else {
            return new \MongoDB\BSON\Timestamp(0, $stamp);
        }
    }

    /**
     * 抛出异常
     * @param $e
     */
    public function showError($e)
    {
        exit($e->getMessage());
    }
}