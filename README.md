# mongodb class


>composer require fifths/mongodb

or

composer.json

    {
       "require": {
       "fifths/mongodb": "*@dev"
      }
    }


>composer udpate

    $m=\Mongodb\Mongodb::getInstance();
    $condition = array(
        '_id' => new MongoId('000000000000000000000123')
    );
    $rs=$m->db->ProductTeamtour->find($condition);