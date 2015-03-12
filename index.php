<?php
require 'vendor/autoload.php';

$app = new \Slim\App(array(
	'debug'=> false,
	'mode' => 'development',
	'http.version' => '1.1'
));

$db = new Cassandra\Connection(['127.0.0.1']);

class JsonResponse extends Http\Response{

	public function json($json){
		return $this->write(json_encode($json));
	}
}

$app['response'] = $app->factory(function ($c) {
	$headers = new Http\Headers(['Content-Type' => 'application/json']);
	$cookies = new Http\Cookies([], [
			'expires' => $c['settings']['cookies.lifetime'],
			'path' => $c['settings']['cookies.path'],
			'domain' => $c['settings']['cookies.domain'],
			'secure' => $c['settings']['cookies.secure'],
			'httponly' => $c['settings']['cookies.httponly'],
			]);
	$response = new JsonResponse(200, $headers, $cookies);

	return $response->withProtocolVersion($c['settings']['http.version']);
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
                ->withBody(new Slim\Http\Body(fopen('php://temp', 'r+')))
                ->json($json);
	};
	return $handler;
};

$app['notFoundHandler'] = function ($c){
	$handler = function (Psr\Http\Message\RequestInterface $request, Psr\Http\Message\ResponseInterface $response)
	{
		$json = [
			'message'=> 'Not Found',
		];

		return $response
			->withStatus(404)
			->withBody(new Slim\Http\Body(fopen('php://temp', 'r+')))
			->json($json);
	};
	return $handler;
};

$app->get('/{keyspace}/query', function ($request, $response, $args) use ($app, $db){
	$db->setKeyspace($args['keyspace']);
	$cql = $request->getQueryParams('cql');
	$rows = $db->querySync($cql)->fetchAll();
	
	$response->json($rows->toArray());
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
	
	$preparedData = $select->prepare();
	
	$bind = [];
	$index = 0;
	foreach($params as $value){
		$bind[] = Cassandra\Type\Base::getTypeObject($preparedData['metadata']['columns'][$index]['type'], $value);
		++$index;
	}
	
	$rows = $db->executeSync($preparedData['id'], $bind)->fetchAll();
	
	$response->json($rows->toArray());
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
	
	$preparedData = FluentCQL\Query::update($args['table'])
		->set(implode(', ', $assignments))
		->where(implode(' AND ', $conditions))
		->prepare();
	
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
	
	$response->json($result->getData());
});

$app->post('/{keyspace}/{table}', function ($request, $response, $args) use ($app, $db) {
	$db->setKeyspace($args['keyspace']);
	$data = $request->getParsedBody();
	
	$preparedData = FluentCQL\Query::insertInto($args['table'])
		->clause('(' . \implode(', ', \array_keys($data)) . ')')
		->values('(' . \implode(', ', \array_fill(0, count($data), '?')) . ')')
		->prepare();
	
	$bind = [];
	$index = 0;
	foreach($data as $value){
		$bind[] = Cassandra\Type\Base::getTypeObject($preparedData['metadata']['columns'][$index]['type'], $value);
		++$index;
	}
	
	$result = $db->executeSync($preparedData['id'], $bind);
	
	$response->json($result->getData());
});

$app->delete('/{keyspace}/{table}', function ($request, $response, $args) use ($app, $db) {
	$db->setKeyspace($args['keyspace']);
	$params = $request->getQueryParams();
	
	$conditions = [];
	foreach($params as $columnName => $value)
		$conditions[] = $columnName . ' = ?';
	
	$preparedData = FluentCQL\Query::delete()->from($args['table'])
		->where(implode(' AND ', $conditions))
		->prepare();
	$bind = [];
	$index = 0;
	foreach($params as $value){
		$bind[] = Cassandra\Type\Base::getTypeObject($preparedData['metadata']['columns'][$index]['type'], $value);
		++$index;
	}
	
	$result = $db->executeSync($preparedData['id'], $bind);
	
	$response->json($result->getData());
});

$app->run();
