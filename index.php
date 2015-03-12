<?php
require 'vendor/autoload.php';

$app = new \Slim\App(array(
	'debug'=> false,
	'mode' => 'development',
	'http.version' => '1.1'
));

$db = new Cassandra\Connection(['127.0.0.1']);

$app->add(function($request, $response, $next){
	$next($request, $response);
	
	return $response->withHeader('Content-type', 'application/json');
});

$app['errorHandler'] = function ($c){
	$handler = function (Psr\Http\Message\RequestInterface $request, Psr\Http\Message\ResponseInterface $response, \Exception $e)
	{
		$json = [
			'type' => get_class($e),
			'code' => $e->getCode(),
			'message'=>$e->getMessage(),
		];
		
		//$json['file'] = $e->getFile();
		//$json['line'] = $e->getLine();
		//$json['trace'] = $e->getTrace();
		
		return $response
                ->withStatus(500)
                ->withHeader('Content-type', 'application/json')
                ->withBody(new Slim\Http\Body(fopen('php://temp', 'r+')))
                ->write(json_encode($json));
	};
	return $handler;
};

$app->get('/{keyspace}/query', function ($request, $response, $args) use ($app, $db){
	$db->setKeyspace($args['keyspace']);
	$cql = $request->getQueryParams('cql');
	$rows = $db->querySync($cql)->fetchAll();
	
	$response->write(json_encode($rows->toArray()));
});

$app->get('/{keyspace}/{table}', function ($request, $response, $args) use ($app, $db) {
	$db->setKeyspace($args['keyspace']);
	$params = $request->getQueryParams();
	
	$select = FluentCQL\Query::select('*')->from($args['table']);
	
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
	
	$response->write(json_encode($rows->toArray()));
});

$app->patch('/{keyspace}/{table}', function ($request, $response, $args) use ($app, $db) {
	$db->setKeyspace($args['keyspace']);
	$params = $request->getQueryParams();
	$data = $request->getParsedBody();
	
	$assignments = [];
	foreach($data as $columnName => $value)
		$assignments[] = $columnName . ' = ?';
	
	$conditions = [];
	foreach($params as $columnName => $value)
		$conditions[] = $columnName . ' = ?';
	
	$query = FluentCQL\Query::update($args['table'])
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
	
	$response->write(json_encode($result->getData()));
});

$app->post('/{keyspace}/{table}', function ($request, $response, $args) use ($app, $db) {
	$db->setKeyspace($args['keyspace']);
	$data = $request->getParsedBody();
	
	$query = FluentCQL\Query::insertInto($args['table'])
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
	
	$response->write(json_encode($result->getData()));
});

$app->delete('/{keyspace}/{table}', function ($request, $response, $args) use ($app, $db) {
	$db->setKeyspace($args['keyspace']);
	$params = $request->getQueryParams();
	
	$conditions = [];
	foreach($params as $columnName => $value)
		$conditions[] = $columnName . ' = ?';
	
	$select = FluentCQL\Query::delete()->from($args['table']);
	
	$preparedData = $db->prepare($select->assemble());
	$bind = [];
	$index = 0;
	foreach($params as $value){
		$bind[] = Cassandra\Type\Base::getTypeObject($preparedData['metadata']['columns'][$index]['type'], $value);
		++$index;
	}
	
	$result = $db->executeSync($preparedData['id'], $bind);
	
	$response->write(json_encode($result->getData()));
});

$app->run();
