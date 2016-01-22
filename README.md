# mongodb class

```
composer require fifths/mongodb
```

```php

$m=\Mongodb\Mongodb::getInstance();
$condition = array(
    '_id' => new MongoId('000000000000000000000123')
);
$rs=$m->db->ProductTeamtour->find($condition);
```