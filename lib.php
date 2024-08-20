<?php
// Função para verificar se os arquivos existem
function verifyFiles($pacientes, $agendamentos) {
    return file_exists($pacientes) && file_exists($agendamentos);
}

// Função para executar a consulta SQL
function executeQuery($conn, $query, $successMessage, $errorMessage) {
    if ($conn->query($query) === TRUE) {
        echo $successMessage . "\n";
    } else {
        echo $errorMessage . ": " . $conn->error . "\n";
    }
}

// Função para obter a data atual
function dateNow(){
    date_default_timezone_set('America/Sao_Paulo');
    return date('d-m-Y \à\s H:i:s');
  }

// Função para desconectar do banco de dados
function disconnectDatabase($conn) {
    if ($conn) {
        mysqli_close($conn);
    }
}

// Função para conectar ao banco de dados
function connectDatabase($host, $user, $password, $db, $port) {
    $conn = mysqli_connect($host, $user, $password, $db, $port);
    if (!$conn) {
        die("Conexão falhou: " . mysqli_connect_error());
    }
    return $conn;
}

// Função para substituir collation no arquivo dump
function replaceCollation($filePath, $oldCollation, $newCollation) {
    $content = file_get_contents($filePath);
    $content = str_replace($oldCollation, $newCollation, $content);
    file_put_contents($filePath, $content);
}



?>
