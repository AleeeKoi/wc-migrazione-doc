<?php

$st = time();

set_time_limit(540);

require('vendor/autoload.php');
require('./configStaging.php');
require('./functions.php');

$from = $conf_query_protocolli::$from;
$to = $conf_query_protocolli::$to;
$limit = $conf_query_protocolli::$limit;

if (!validateDate($from)) {
    exit('from date [' . $from . '] not valid (YYYY-MM-DD)');
}

if (!validateDate($to)) {
    exit('to date [' . $to . '] not valid (YYYY-MM-DD)');
}

if ($from > $to) {
    exit('from date[' . $from . '] less then to date [' . $to . ']');
}

if (!is_numeric($limit)) {
    exit('limit non numerico [' . $limit . ']');
}

$samba = samba($conf_samba);

$s3Client = s3($conf_s3);

$pdo = new PDO("mysql:host=" . $conf_db::$host, $conf_db::$user, $conf_db::$password, $conf_db::$options);

$credentialsS3 = new \Aws\Credentials\Credentials($conf_s3::$aws_access_key_id, $conf_s3::$aws_secret_access_key_id);
            
// Let's construct our S3EncryptionClient using an S3Client
$encryptionClient = new \Aws\S3\Crypto\S3EncryptionClientV2(
    new \Aws\S3\S3Client([
        'region' => $conf_s3::$aws_region,
        'version' => $conf_s3::$aws_version,
        'credentials' => $credentialsS3,
    ])
);

$materialsProvider = new \Aws\Crypto\KmsMaterialsProviderV2(
    new \Aws\Kms\KmsClient([
        'region' => $conf_s3::$aws_region,
        'version' => $conf_s3::$aws_version,
        'credentials' => $credentialsS3,
    ]),
    $conf_s3::$kms_key
);

$cipherOptions = [
    'Cipher' => 'gcm',
    'KeySize' => 256,
];

$faschim_protocolli_pdo = $pdo->query("
    select
        idprotocollo,
        codmittente,
        NumeroProtocollo,
        DataDocumento,
        codicerichiestarimborso,
        codfattura
    from " . $conf_query_protocolli::$schema . ".protocolli
    join " . $conf_query_protocolli::$schema . ".protocollirichieste on protocolli.IdProtocollo = protocollirichieste.CodiceProtocollo
    where CodFattura is not null
      and dataDocumento >= '" . $from . "'
      and dataDocumento <= '" . $to . "'
      and Importato = 0
    order by dataDocumento DESC
    limit " . $limit
);

echo PHP_EOL . 'record estratti faschim protocolli: ' . $faschim_protocolli_pdo->rowCount();

// definisco contenuto e path dei csv

$time = time();

$csv_ok_content = '"idprotocollo";"codmittente";"NumeroProtocollo";"DataDocumento";"codicerichiestarimborso";"codfattura";"savedPath";';

$csv_ok_path = 'DocumentiTestImport/csv/ok_' . $time . '_' . $from . '_' . $to . '.csv';

$csv_ko_content = '"idprotocollo";"codmittente";"NumeroProtocollo";"DataDocumento";"codicerichiestarimborso";"codfattura";"error";';

$csv_ko_path = 'DocumentiTestImport/csv/ko_' . $time . '_' . $from . '_' . $to . '.csv';

$protocolli_fetch = $faschim_protocolli_pdo->fetchAll();
$array_id_protocolli = array_column($protocolli_fetch, 'idprotocollo');
$array_cod_fattura = array_column($protocolli_fetch, 'codfattura');

if (count($array_cod_fattura) === 0) {
    sns_publish($conf_sns, 'ERROR: array_cod_fattura vuoto');
    exit('ERROR: array_cod_fattura vuoto');
}

$staging_documenti_pdo = $pdo->query("
        select
            Documenti.idDocumento,
            Documenti.idPratica,
            Documenti.numeroDocumento,
            Documenti.dataDocumento,
            Documenti.pathDocumento,
            Documenti.nomeFile,
            Documenti.location,
            Pratica.idPersonaSocio,
            Pratica.numeroProtocollo,
            Persona.codiceFiscale,    
            Documenti.idFatturaOriginale
        from " . $conf_query_documenti::$schema . ".Documenti
        left join " . $conf_query_documenti::$schema . ".Pratica on Documenti.idPratica = Pratica.idPratica
        left join " . $conf_query_documenti::$schema . ".Persona on Persona.idPersona = Pratica.idPersonaSocio
        where Documenti.idFatturaOriginale in (" . implode(',', $array_cod_fattura) . ")"
);

echo PHP_EOL . 'record estratti staging documenti: ' . $staging_documenti_pdo->rowCount();

foreach($protocolli_fetch as $row) {

    $trovato = false;

    echo PHP_EOL . PHP_EOL . '--  idprotocollo:' . $row['idprotocollo'] . ' - codfattura: ' . $row['codfattura'];

    foreach ($staging_documenti_pdo as $doc) {

        if ($doc['idFatturaOriginale'] != $row['codfattura']) {
            continue; // vado al ciclo successivo
        }

        $trovato = true;

        if ($doc['codiceFiscale'] === null && strlen($doc['codiceFiscale']) === 0) {
            if ($verbose) {
                echo PHP_EOL . '----  ERROR: codice fiscale inesistente per l\'idDocumento ' . $doc['idDocumento'];
            }
            compila_riga_csv_KO("codice fiscale inesistente");
            break;
        }

        if ($doc['numeroProtocollo'] === null && strlen($doc['numeroProtocollo']) === 0) {
            if ($verbose) {
                echo PHP_EOL . '----  ERROR: numero protocollo inesistente per l\'idDocumento ' . $doc['idDocumento'];
            }
            compila_riga_csv_KO("numero protocollo inesistente");
            break;
        }

        $parts = explode('-', $row['DataDocumento']);

        $anno = $parts[0];

        $mese = $parts[1];

        $giorno = $parts[2];

        $path_doc_origin = 'ProtocolloFaschim/Prestazioni/' . $anno . '/' . $mese . '/' . $giorno . '/' . $row['NumeroProtocollo'] . '.PDF';

        if ($samba->has($path_doc_origin)) {
            if ($verbose) {
                echo PHP_EOL . '----  file in samba presente: ' . $path_doc_origin;
            }
        } else {
            if ($verbose) {
                echo PHP_EOL . '----  ERROR: file in samba non presente: ' . $path_doc_origin;
            }
            compila_riga_csv_KO('file in samba non presente: ' . $path_doc_origin);
            break;
        }

        try {
            $read_file = $samba->read($path_doc_origin);
        } catch (\Throwable $ex) {
            echo_exception($ex);
            compila_riga_csv_KO('EXCEPTION: ' . $ex->getMessage());
            break;
        }

        $path_doc_dirs = [
            $conf_dir_destination::$root,
            'Documenti',
            'Persona',
            $doc['codiceFiscale'],
            'Pratiche',
            $doc['numeroProtocollo'],
        ];

        $path_doc_dir = implode(DIRECTORY_SEPARATOR, $path_doc_dirs);

        $name_doc = $row['NumeroProtocollo'] . '.PDF';

        $path_doc_destination = $path_doc_dir . DIRECTORY_SEPARATOR . $name_doc;

        try {
            
            $result = $encryptionClient->putObject([
                '@MaterialsProvider' => $materialsProvider,
                '@CipherOptions' => $cipherOptions,
                '@KmsEncryptionContext' => [],
                'Bucket' => $conf_s3::$aws_bucket,
                'Key' => $path_doc_destination,
                'Body' => $read_file,
                'ACL' =>  $conf_s3::$aws_acl,
            ]);

            if ($verbose) {
                echo PHP_EOL . '----  file saved successfully in S3 path: ' . $result->get('ObjectURL');
            }

            $update_doc = "
                UPDATE " . $conf_query_documenti::$schema . ".Documenti 
                SET pathDocumento = ?,
                    nomeFile = ?, 
                    location = 'S3'
                    modified = NOW()
                    userId = 'MIGRAZIONE9'
                WHERE idDocumento = ?";

            $pdo->prepare($update_doc)->execute([$path_doc_dir, $name_doc, $doc['idDocumento']]);

            // compilo il csv ok

            $csv_ok_content .= PHP_EOL .
                '"' . $row['idprotocollo'] . '";' .
                '"' . $row['codmittente'] . '";' .
                '"' . $row['NumeroProtocollo'] . '";' .
                '"' . $row['DataDocumento'] . '";' .
                '"' . $row['codicerichiestarimborso'] . '";' .
                '"' . $row['codfattura'] . '";' .
                '"' . $result->get('ObjectURL') . '"';

        } catch (\Throwable $ex) {
            echo_exception($ex);
            sns_publish($conf_sns, $ex->getMessage());
            compila_riga_csv_KO('EXCEPTION: ' . $ex->getMessage());
        }
    }

    if (!$trovato) {
        compila_riga_csv_KO("Record in Documenti non trovato");
    }
}

try {
    $in  = str_repeat('?,', count($array_id_protocolli) - 1) . '?';

    $update_protocollo =  "
    UPDATE " . $conf_query_protocolli::$schema . ".protocolli 
    SET Importato = 1
    WHERE idprotocollo in ($in)";

    $pdo->prepare($update_protocollo)->execute($array_id_protocolli);

} catch (\Throwable $ex) {
    echo_exception($ex);
    sns_publish($conf_sns, $ex->getMessage());
}

if ($faschim_protocolli_pdo->rowCount()) {

    $result_csv_ok = $s3Client->putObject([
        'Bucket' => $conf_s3::$aws_bucket,
        'Key'    => $csv_ok_path,
        'Body'   => $csv_ok_content,
        'ACL'    =>  $conf_s3::$aws_acl,
    ]);

    if ($verbose) {
        echo PHP_EOL . 'file csv OK saved successfully in S3 path: ' . $result_csv_ok->get('ObjectURL');
    }

    $result_csv_ko = $s3Client->putObject([
        'Bucket' => $conf_s3::$aws_bucket,
        'Key'    => $csv_ko_path,
        'Body'   => $csv_ko_content,
        'ACL'    => $conf_s3::$aws_acl,
    ]);

    if ($verbose) {
        echo PHP_EOL . 'file csv KO saved successfully in S3 path: ' . $result_csv_ko->get('ObjectURL');
    }
}

$et = time();

if ($verbose) {
    echo PHP_EOL . 'execution time: ' . ($et - $st) . ' seconds';
}

echo PHP_EOL;
echo PHP_EOL;
echo PHP_EOL;
