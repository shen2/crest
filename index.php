<?php
require 'vendor/autoload.php';

$app = new \Slim\Slim(array(
	'debug'=> false,
	'mode' => 'development',
	'http.version' => '1.1'
));

$db = new Cassandra\Connection(['127.0.0.1']);

class JsonHeaderMiddleware extends \Slim\Middleware
{
	public function call(){
		$app = $this->app;

		$this->next->call();

		$app->response->headers['Content-Type'] = 'application/json';
	}
}

$app->add(new \JsonHeaderMiddleware());

$app->error(function (\Exception $e) use ($app) {
	$json = [
		'type' => get_class($e),
		'code' => $e->getCode(),
		'message'=>$e->getMessage(),
	];
	
	//$json['file'] = $e->getFile();
	//$json['line'] = $e->getLine();
	//$json['trace'] = $e->getTrace();
	
	$app->response->write(json_encode($json));
});

$app->get('/:keyspace/query', function ($keyspace) use ($app, $db){
	$db->setKeyspace($keyspace);
	$cql = $app->request->get('cql');
	$rows = $db->querySync($cql)->fetchAll();
	
	$app->response->write(json_encode($rows->toArray()));
});

$app->get('/:keyspace/:table', function ($keyspace, $table) use ($app, $db) {
	$db->setKeyspace($keyspace);
	$params = $app->request->get();
	
	$select = FluentCQL\Query::select('*')->from($table);
	
	if (!empty($params)){
		$conditions = [];
		foreach($params as $columnName => $value)
			$conditions[] = $columnName . ' = ?';
		
		$select->where(implode(' AND ', $conditions));
	}
	
	$preparedData = $db->prepare($select->assemble());
	$bind = [];
	$index = 0;
	foreach($params as $value){
		$bind[] = Cassandra\Type\Base::getTypeObject($preparedData['metadata']['columns'][$index]['type'], $value);
		++$index;
	}
	
	$rows = $db->executeSync($preparedData['id'], $bind)->fetchAll();
	
	$app->response->write(json_encode($rows->toArray()));
});

$app->patch('/:keyspace/:table', function ($keyspace, $table) use ($app, $db) {
	$db->setKeyspace($keyspace);
	$params = $app->request->get();
	$data = $app->request->patch();
	
	$assignments = [];
	foreach($data as $columnName => $value)
		$assignments[] = $columnName . ' = ?';
	
	$conditions = [];
	foreach($params as $columnName => $value)
		$conditions[] = $columnName . ' = ?';
	
	$query = FluentCQL\Query::update($table)
		->set(implode(', ', $assignments))
		->where(implode(' AND ', $conditions));
	
	$preparedData = $db->prepare($query->assemble());
	
	$bind = [];
	$index = 0;
	foreach($data as $value){
		$bind[] = Cassandra\Type\Base::getTypeObject($preparedData['metadata']['columns'][$index]['type'], $value);
		++$index;
	}
	foreach($params as $value){
		$bind[] = Cassandra\Type\Base::getTypeObject($preparedData['metadata']['columns'][$index]['type'], $value);
		++$index;
	}
	
	$result = $db->executeSync($preparedData['id'], $bind);
	
	$app->response->write(json_encode($result->getData()));
});

$app->post('/:keyspace/:table', function ($keyspace, $table) use ($app, $db) {
	$db->setKeyspace($keyspace);
	$data = $app->request->post();
	
	$query = FluentCQL\Query::insertInto($table)
		->clause('(' . \implode(', ', \array_keys($data)) . ')')
		->values('(' . \implode(', ', \array_fill(0, count($data), '?')) . ')');
	
	$preparedData = $db->prepare($query->assemble());
	
	$bind = [];
	$index = 0;
	foreach($data as $value){
		$bind[] = Cassandra\Type\Base::getTypeObject($preparedData['metadata']['columns'][$index]['type'], $value);
		++$index;
	}
	
	$result = $db->executeSync($preparedData['id'], $bind);
	
	$app->response->write(json_encode($result->getData()));
});

$app->delete('/:keyspace/:table', function ($keyspace, $table) use ($app, $db) {
	$db->setKeyspace($keyspace);
	$params = $app->request->get();
	
	$conditions = [];
	foreach($params as $columnName => $value)
		$conditions[] = $columnName . ' = ?';
	
	$select = FluentCQL\Query::delete()->from($table);
	
	$preparedData = $db->prepare($select->assemble());
	$bind = [];
	$index = 0;
	foreach($params as $value){
		$bind[] = Cassandra\Type\Base::getTypeObject($preparedData['metadata']['columns'][$index]['type'], $value);
		++$index;
	}
	
	$result = $db->executeSync($preparedData['id'], $bind);
	
	$app->response->write(json_encode($result->getData()));
});

$app->run();
