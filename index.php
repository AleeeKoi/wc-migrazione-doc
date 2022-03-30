<?php

$st = time();

set_time_limit(0);

require('vendor/autoload.php');
require('./config.php');
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

$fp = fopen(__DIR__ . DIRECTORY_SEPARATOR . 'lock', 'w');

if (!flock($fp, LOCK_EX | LOCK_NB)) {
    echo 'Unable to obtain lock';
    exit(-1);
}

$faschim_protocolli_pdo = $pdo->query("
    select
        IdProtocollo,
        NumeroProtocollo,
        DataDocumento,
        CodiceRichiestaRimborso,
        CodOggetto
    from " . $conf_query_protocolli::$schema . ".protocolli
    join " . $conf_query_protocolli::$schema . ".protocollirichieste on protocolli.IdProtocollo = protocollirichieste.CodiceProtocollo
    -- join " . $conf_query_protocolli::$schema . ".oggetti on protocolli.CodOggetto = oggetti.IdOggetto
    join " . $conf_query_protocolli::$schema . ".richiesterimborso on protocollirichieste.CodiceRichiestaRimborso = richiesterimborso.IdRichiestaRimborso
    where Importato = 0
      and dataDocumento >= '" . $from . "'
      and dataDocumento <= '" . $to . "'
      and CodOggetto is not null
    order by dataDocumento DESC
    limit " . $limit
);

echo PHP_EOL . 'record estratti faschim protocolli: ' . $faschim_protocolli_pdo->rowCount();

// definisco contenuto e path dei csv

$time = time();

$csv_ok_content = '"IdProtocollo";"NumeroProtocollo";"DataDocumento";"CodiceRichiestaRimborso";"CodOggetto";"savedPath";';
$csv_ok_path = 'DocumentiTestImport/csv/ok_' . $time . '_' . $from . '_' . $to . '.csv';

$csv_ko_content = '"IdProtocollo";"NumeroProtocollo";"DataDocumento";"CodiceRichiestaRimborso";"CodOggetto";"error";';
$csv_ko_path = 'DocumentiTestImport/csv/ko_' . $time . '_' . $from . '_' . $to . '.csv';

$protocolli_fetch = $faschim_protocolli_pdo->fetchAll();
$array_id_protocolli = array_column($protocolli_fetch, 'IdProtocollo');
$array_richieste_rimborsi = array_column($protocolli_fetch, 'CodiceRichiestaRimborso');

if (count($array_richieste_rimborsi) === 0) {
    sns_publish($conf_sns, 'INFO: array_richieste_rimborsi vuoto (finito? Samuel stacca il cron...)');
    fclose($fp);
    exit('INFO: array_richieste_rimborsi vuoto (finito?)');
}

$pratiche_pdo = $pdo->query("
    select
        Pratica.idPratica,
        Pratica.numeroProtocollo,
        Persona.codiceFiscale
    from " . $conf_query_documenti::$schema . ".Pratica
    join " . $conf_query_documenti::$schema . ".Persona on Persona.idPersona = Pratica.idPersonaSocio
    where Pratica.numeroProtocollo in ('" . implode("','", $array_richieste_rimborsi) . "')
");

echo PHP_EOL . 'record estratti staging pratiche: ' . $pratiche_pdo->rowCount();

$pratiche_pdo_rows = $pratiche_pdo->fetchAll();

foreach($protocolli_fetch as $protocollo_orig) {

    $trovato = false;

    echo PHP_EOL . PHP_EOL . '--  IdProtocollo:' . $protocollo_orig['IdProtocollo'] . ' -- CodiceRichiestaRimborso ' . $protocollo_orig['CodiceRichiestaRimborso'];

    foreach ($pratiche_pdo_rows as $pratica_dest) {

        if ($trovato) {
            break;
        }

        if (intval($pratica_dest['numeroProtocollo']) !== intval($protocollo_orig['CodiceRichiestaRimborso'])) {
            continue; // vado al ciclo successivo
        }

        $trovato = true;

        if ($pratica_dest['codiceFiscale'] === null && strlen($pratica_dest['codiceFiscale']) === 0) {
            if ($verbose) {
                echo PHP_EOL . '----  ERROR: codice fiscale inesistente per protocollo ' . $pratica_dest['numeroProtocollo'];
            }
            compila_riga_csv_KO("codice fiscale inesistente");
            break;
        }

        if ($pratica_dest['numeroProtocollo'] === null && strlen($pratica_dest['numeroProtocollo']) === 0) {
            if ($verbose) {
                echo PHP_EOL . '----  ERROR: numero protocollo inesistente per protocollo ' . $pratica_dest['numeroProtocollo'];
            }
            compila_riga_csv_KO("numero protocollo inesistente");
            break;
        }

        $parts = explode('-', $protocollo_orig['DataDocumento']);
        $anno = $parts[0] ?? null;
        $mese = $parts[1] ?? null;
        $giorno = $parts[2] ?? null;

        if (!$anno || !$mese || !$giorno) {
            if ($verbose) {
                echo PHP_EOL . '----  ERROR: formato data errato ' . $protocollo_orig['DataDocumento'];
            }
            compila_riga_csv_KO('formato data errato ' . $protocollo_orig['DataDocumento']);
            break;
        }

        $file_name = $protocollo_orig['NumeroProtocollo'] . '.PDF';
        $path_doc_origin = 'ProtocolloFaschim/Prestazioni/' . $anno . '/' . $mese . '/' . $giorno . '/' . $file_name;

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
            'Persona',
            $pratica_dest['codiceFiscale'],
            'Pratiche',
            $pratica_dest['numeroProtocollo'],
        ];

        $path_doc_dir = implode(DIRECTORY_SEPARATOR, $path_doc_dirs);
        $path_doc_destination = $path_doc_dir . DIRECTORY_SEPARATOR . $file_name;

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
                INSERT " . $conf_query_documenti::$schema . ".Documenti 
                SET idPratica = ?,
                    tipoDocumentoId = ?,
                    pathDocumento = ?,
                    nomeFile = ?,
                    location = 'S3',
                    tipoDocumento = 999,
                    modified = NOW(),
                    userId = 'MIGRAZIONEDOC1',
                    migrazione = 1";

            $pdo->prepare($update_doc)->execute([
                $pratica_dest['idPratica'],
                intval($protocollo_orig['CodOggetto']) + 1000,
                $path_doc_dir,
                $file_name
            ]);

            // compilo il csv ok

            $csv_ok_content .= PHP_EOL .
                '"' . $protocollo_orig['IdProtocollo'] . '";' .
                '"' . $protocollo_orig['NumeroProtocollo'] . '";' .
                '"' . $protocollo_orig['DataDocumento'] . '";' .
                '"' . $protocollo_orig['CodiceRichiestaRimborso'] . '";' .
                '"' . $protocollo_orig['CodOggetto'] . '";' .
                '"' . $result->get('ObjectURL') . '"';

        } catch (\Throwable $ex) {
            echo_exception($ex);
            sns_publish($conf_sns, $ex->getMessage());
            compila_riga_csv_KO('EXCEPTION: ' . $ex->getMessage());
        }
    }

    if (!$trovato) {
        echo PHP_EOL . '----  ERROR: Record in Pratiche non trovato ' . $protocollo_orig['CodiceRichiestaRimborso'];
        compila_riga_csv_KO("Record in Pratiche non trovato");
    }

    $update_protocollo = "
        UPDATE " . $conf_query_protocolli::$schema . ".protocolli 
        SET Importato = 1
        WHERE IdProtocollo = ?";

    $pdo->prepare($update_protocollo)->execute([$protocollo_orig['IdProtocollo']]);
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

fclose($fp);


/**
 * query da eseguire:
 * ALTER TABLE Documenti ADD migrazione TINYINT(1) DEFAULT 0 NOT NULL AFTER nomeFile;
 * CREATE INDEX Documenti_migrazione_IDX USING BTREE ON Documenti (migrazione);
 */