<?php
require 'vendor/autoload.php';

$app = new \Slim\Slim(array(
    'debug'=> true,
    'mode' => 'development',
    'http.version' => '1.1'
));

$db = new Cassandra\Connection(['127.0.0.1']);

$app->get('/:keyspace/query', function ($keyspace) use ($app, $db){
    $db->setKeyspace($keyspace);
    $cql = $app->request->get('cql');
    $result = $db->querySync($cql);
    $app->response->write(json_encode($result->fetchAll()->toArray()));
});


$app->get('/:keyspace/:table', function ($keyspace, $table) use ($app, $db) {
    $db->setKeyspace($keyspace);
    $params = $app->request->get();
    $select = FluentCQL\Query::select('*')->from($table);
    $conditions = 0;
    foreach($params as $columnName => $value){
        if ($conditions ++ === 0)
            $select->where("\"$columnName\" = ?", $value);
        else
            $select->and("\"$columnName\" = ?", $value);
    }
    $select->limit(10);
    $preparedData = $db->prepare($select->assemble());
    $strictValues = Cassandra\Request\Request::strictTypeValues($select->getBind(), $preparedData['metadata']['columns']);
    $result = $db->executeSync($preparedData['id'], $strictValues);
    $app->response->write(json_encode($result->fetchAll()->toArray()));
});

$app->post('/:keyspace/:table', function ($keyspace, $table) use ($app, $db) {
    // TODO
});

$app->delete('/:keyspace/:table', function ($keyspace, $table) use ($app, $db) {
    // TODO
});

$app->run();
