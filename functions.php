<?php

function validateDate($date, $format = 'Y-m-d') {
    $d = DateTime::createFromFormat($format, $date);
    return $d && $d->format($format) == $date;
}

function samba($conf) {
    $factory = new \Icewind\SMB\ServerFactory;
    $auth = new \Icewind\SMB\BasicAuth($conf::$user, $conf::$workgroup, $conf::$password);
    $server = $factory->createServer($conf::$server, $auth);
    $share = $server->getShare('temp');
    return new \League\Flysystem\Filesystem(new \RobGridley\Flysystem\Smb\SmbAdapter($share));
}

function s3($conf) {
    $credentials = new \Aws\Credentials\Credentials($conf::$aws_access_key_id, $conf::$aws_secret_access_key_id);
    $options = [
        'region' => $conf::$aws_region,
        'version' => $conf::$aws_version,
        'credentials' => $credentials,
    ];

    return new \Aws\S3\S3Client($options);
}

function sns_publish($conf, $message) {
    $credentials = new Aws\Credentials\Credentials($conf::$aws_access_key_id, $conf::$aws_secret_access_key_id);

    $SnSclient = new \Aws\Sns\SnsClient([
        'region' => $conf::$aws_region,
        'credentials' => $credentials,
        'version' => $conf::$aws_version
    ]);

    try {
        $result = $SnSclient->publish([
            'Message' => $message,
            //'PhoneNumber' => $conf::$number,
            'TopicArn' => $conf::$topicArn,
        ]);
    } catch (\Aws\Exception\AwsException $e) {
        // output error message if fails
        echo_exception($e);
    }
}

function echo_exception(\Throwable $ex) {
    echo PHP_EOL . '----  EXCEPTION';
    echo PHP_EOL . '----  message: ' . $ex->getMessage();
    echo PHP_EOL . '----  file: ' . $ex->getFile();
    echo PHP_EOL . '----  line: ' . $ex->getLine();
    echo PHP_EOL . '----  trace: ' . $ex->getTraceAsString();
}

function compila_riga_csv_KO($error) {
    global $csv_ko_content, $protocollo_orig;
    $csv_ko_content .= PHP_EOL .
    '"' . $protocollo_orig['IdProtocollo'] . '";' .
    '"' . $protocollo_orig['NumeroProtocollo'] . '";' .
    '"' . $protocollo_orig['DataDocumento'] . '";' .
    '"' . $protocollo_orig['CodiceRichiestaRimborso'] . '";' .
    '"' . $protocollo_orig['CodOggetto'] . '";' .
    '"' . $error . '"';
}
