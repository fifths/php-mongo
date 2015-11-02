<?php

require '../vendor/autoload.php';

$m=\Mongodb\Mongodb::getInstance();
$condition = array(
    '_id' => new MongoId('000000000000000000000537')
);
$rs=$m->db->ProductTeamtour->find($condition);

$data=array();
foreach($rs as $v)
{
    $data[] = $v;
}

echo json_encode($data);
