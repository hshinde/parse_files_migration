<?php


$CWD="<CURRENT WORKING DIRECTORY>"
require "$CWD/vendor/autoload.php";
use Aws\S3\S3Client;

// GET Parse database schema

$output = shell_exec('curl -X GET   -H "X-Parse-Application-Id:< APP ID >" -H "X-Parse-Master-Key: < MASTER KEY >" -G https://api.parse.com/1/schemas/');
$schema = json_decode($output);

// Create S3 client

$aws = S3Client::factory(array(
            'version' => 'latest',
            'region' => '<REGION>',
            'credentials' => array(
               'key' => "< ACCESS_KEY >",
               'secret' => "< SECRET_KEY >"
            )
     ));

$bucket="< BUCKET_NAME >"
$filekey="< PARSE FILE KEY>"
$mongo_conn_str="<MONGODB CONNECTION STRING>"
function moveToS3($class, $field){
   global $aws, $bucket, $CWD, $filekey, $mongo_conn_str;
   $manager = new MongoDB\Driver\Manager($mongo_conn_str);
   $filter = [];
   $options = [$field];
   $query = new MongoDB\Driver\Query($filter, $options);
   $cursor = $manager->executeQuery("$dbname.$class", $query);
   $bulk = new MongoDB\Driver\BulkWrite(['ordered' => true]);
   $it = new \IteratorIterator($cursor);
   $it->rewind(); // Very important

   while($doc = $it->current()) {
      $it->next();
      if(!isset($doc->$field)) {
        continue;
      }
         

      $text = preg_replace("/https:\/\/s3.amazonaws.com\/$bucket\//",'',$doc->$field);
      echo $field . " " . $doc->$field . " " . $text . "\n";
      $idtag = "_id";
      #var_dump($doc);
      set_time_limit(0); // unlimited max execution time
      $url = "http://files.parsetfss.com/$filekey/" . $doc->$field;
      
      $fp = fopen("./temp/" . $doc->$field, "w");
      $options = array(
        CURLOPT_FILE    => $fp,
        CURLOPT_TIMEOUT =>  28800, // set this to 8 hours so we dont timeout on big files
        CURLOPT_URL     => $url, 
      );
      $ch = curl_init();
      curl_setopt_array($ch, $options);
      curl_exec($ch);
      $http_code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
      curl_close($ch);
      echo "File " . $doc->$field . "downloaded with $http_code";
     if($http_code != 200 ) {
        continue;
      }
       $srcfile = "$CWD/temp/" . $doc->$field;
       $result = $aws->putObject(array(
          'Bucket'     => $bucket,
          'Key'        => $doc->$field, 
          'SourceFile' => $srcfile
          )
       );
       $s3path = "https://s3.amazonaws.com/$bucket/" . $doc->$field;
       try {
          $bulk->update(['_id' => $doc->$idtag], ['$set' => [ $field => $text ]]);
          $writeConcern = new MongoDB\Driver\WriteConcern(MongoDB\Driver\WriteConcern::MAJORITY, 1000);
          $result = $manager->executeBulkWrite("$dbname.$class", $bulk, $writeConcern);
       } catch (MongoDB\Driver\Exception\BulkWriteException $e) {
           $result = $e->getWriteResult();
       
           // Check if the write concern could not be fulfilled
           if ($writeConcernError = $result->getWriteConcernError()) {
                   printf("%s (%d): %s\n",
                           $writeConcernError->getMessage(),
                           $writeConcernError->getCode(),
                           var_export($writeConcernError->getInfo(), true)
                   );
               }
       
           // Check if any write operations did not complete at all
           foreach ($result->getWriteErrors() as $writeError) {
                   printf("Operation#%d: %s (%d)\n",
                               $writeError->getIndex(),
                               $writeError->getMessage(),
                               $writeError->getCode()
                   );
               }
       } catch (MongoDB\Driver\Exception\Exception $e) {
           printf("Other error: %s\n", $e->getMessage());
           exit;
       }
 
   }
}


foreach ($schema->results as $table) {
   foreach ($table->fields as $field=>$fval) {
      if($fval->type == "File") {
         echo $table->className . " $field\n";
         moveToS3($table->className, $field);     
      }
   }
}

?>

