<?php
// Importação de Bibliotecas
include "./lib.php";

echo "Início do processo...\n";

// Conexão com o banco da clínica fictícia
$connMedical = connectDatabase("localhost", "root", "root", "MedicalChallenge", 3307)
    or die("Não foi possível conectar ao servidor MySQL: MedicalChallenge\n");

    echo "Conectado com sucesso ao MedicalChallenge\n";

// Conexão com o banco temporário:
$connTemp = connectDatabase("127.0.0.1", "root", "123456", "banco_temporario", "3306")
  or die("Não foi possível conectar ao servidor MySQL: banco_temporario\n");

// Informações de Inicio da Migração:
    echo "Início da Migração: " . dateNow() . ".\n\n";


// Criação das tabelas temporárias no banco de dados temporário
$createTablesQueriesTemp = [
    "CREATE TABLE IF NOT EXISTS pacientes_temp (
        cod_paciente INT(11),
        nome_paciente VARCHAR(255),
        nasc_paciente DATE,
        pai_paciente VARCHAR(255),
        mae_paciente VARCHAR(255),
        cpf_paciente VARCHAR(14),
        rg_paciente VARCHAR(12),
        sexo_pac CHAR(1),
        id_conv INT(11),
        convenio VARCHAR(255),
        obs_clinicas TEXT
    );",
    "CREATE TABLE IF NOT EXISTS agendamentos_temp (
        cod_agendamento INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
        descricao VARCHAR(255),
        dia DATE,
        hora_inicio TIME,
        hora_fim TIME,
        cod_paciente INT(11),
        paciente VARCHAR(255),
        cod_medico INT(11),
        medico VARCHAR(255),
        cod_convenio INT(11),
        convenio VARCHAR(255),
        procedimento VARCHAR(255)
    );"
];

foreach ($createTablesQueriesTemp as $query) {
    executeQuery($connTemp, $query, "Tabela temporária criada com sucesso.", "Erro ao criar tabela temporária");
}

// Caminhos para os arquivos CSV
$pacientes = 'C:/Users/lucas.prigol/Desktop/migration-challenge-main/dados_sistema_legado/20210512_pacientes.csv';
$agendamentos = 'C:/Users/lucas.prigol/Desktop/migration-challenge-main/dados_sistema_legado/20210512_agendamentos.csv';

// Importação dos dados dos CSVs para as tabelas temporárias
$importQueriesTemp = [
    "LOAD DATA INFILE '$pacientes' INTO TABLE pacientes_temp
    FIELDS TERMINATED BY ';' LINES TERMINATED BY '\n' IGNORE 1 LINES
    (cod_paciente, nome_paciente, @nasc_paciente, pai_paciente, mae_paciente, cpf_paciente, rg_paciente, sexo_pac, id_conv, convenio, obs_clinicas)
    SET nasc_paciente = STR_TO_DATE(@nasc_paciente, '%d/%m/%Y');",

    "LOAD DATA INFILE '$agendamentos' INTO TABLE agendamentos_temp
    FIELDS TERMINATED BY ';' LINES TERMINATED BY '\n' IGNORE 1 LINES
    (cod_agendamento, descricao, @dia, hora_inicio, hora_fim, cod_paciente, paciente, cod_medico, medico, cod_convenio, convenio, procedimento)
    SET dia = STR_TO_DATE(@dia, '%d/%m/%Y');"
];

foreach ($importQueriesTemp as $query) {
    executeQuery($connTemp, $query, "Dados importados para a tabela temporária com sucesso.", "Erro ao importar dados para a tabela temporária");
}

// Realização dos ajustes necessários nas tabelas temporárias
$alterTablesQueriesTemp = [
    "ALTER TABLE pacientes_temp 
        DROP COLUMN pai_paciente, 
        DROP COLUMN mae_paciente, 
        DROP COLUMN convenio;",
    "ALTER TABLE pacientes_temp 
        MODIFY COLUMN sexo_pac VARCHAR(10) NOT NULL;",
    "UPDATE pacientes_temp SET sexo_pac = CASE 
        WHEN sexo_pac = 'M' THEN 'Masculino' 
        WHEN sexo_pac = 'F' THEN 'Feminino' 
        ELSE NULL END;",
    "ALTER TABLE pacientes_temp 
        CHANGE COLUMN nome_paciente nome VARCHAR(255) NOT NULL;",
    "ALTER TABLE pacientes_temp 
        CHANGE COLUMN nasc_paciente nascimento DATE NOT NULL;",
    "ALTER TABLE pacientes_temp 
        CHANGE COLUMN cpf_paciente cpf VARCHAR(14) NOT NULL;",
    "ALTER TABLE pacientes_temp 
        CHANGE COLUMN rg_paciente rg VARCHAR(20) NOT NULL;",
    "ALTER TABLE pacientes_temp 
        CHANGE COLUMN sexo_pac sexo ENUM('Masculino','Feminino') NOT NULL;",
    "ALTER TABLE pacientes_temp 
        CHANGE COLUMN id_conv id_convenio INT(11) NOT NULL;",
    "ALTER TABLE pacientes_temp 
        CHANGE COLUMN cod_paciente id INT(11) NOT NULL;",
    "ALTER TABLE pacientes_temp 
        CHANGE COLUMN obs_clinicas cod_referencia VARCHAR(50) NOT NULL;",
    "ALTER TABLE pacientes_temp 
        MODIFY COLUMN nome VARCHAR(255) NOT NULL AFTER id,
        MODIFY COLUMN sexo ENUM('Masculino','Feminino') NOT NULL AFTER nome,
        MODIFY COLUMN nascimento DATE NOT NULL AFTER sexo,
        MODIFY COLUMN cpf VARCHAR(14) NOT NULL AFTER nascimento,
        MODIFY COLUMN rg VARCHAR(20) NOT NULL AFTER cpf,
        MODIFY COLUMN id_convenio INT(11) NOT NULL AFTER rg,
        MODIFY COLUMN cod_referencia VARCHAR(50) NOT NULL AFTER id_convenio;",
    "ALTER TABLE agendamentos_temp 
        DROP COLUMN paciente, 
        DROP COLUMN medico, 
        DROP COLUMN convenio;",
    "ALTER TABLE agendamentos_temp 
        CHANGE COLUMN cod_agendamento id INT(11) NOT NULL;",
    "ALTER TABLE agendamentos_temp 
        CHANGE COLUMN descricao observacoes TEXT;",
    "ALTER TABLE agendamentos_temp 
        CHANGE COLUMN cod_paciente id_paciente INT(11) NOT NULL;",
    "ALTER TABLE agendamentos_temp 
        CHANGE COLUMN cod_medico id_profissional INT(11) NOT NULL;",
    "ALTER TABLE agendamentos_temp 
        CHANGE COLUMN cod_convenio id_convenio INT(11) NOT NULL;",
    "ALTER TABLE agendamentos_temp 
        CHANGE COLUMN procedimento id_procedimento VARCHAR(255) NOT NULL;",
    "ALTER TABLE agendamentos_temp 
        ADD COLUMN dh_inicio DATETIME, 
        ADD COLUMN dh_fim DATETIME;",
    "UPDATE agendamentos_temp 
        SET dh_inicio = CONCAT(dia, ' ', hora_inicio), 
            dh_fim = CONCAT(dia, ' ', hora_fim);",
    "ALTER TABLE agendamentos_temp 
        DROP COLUMN dia, 
        DROP COLUMN hora_inicio, 
        DROP COLUMN hora_fim;",
    "ALTER TABLE agendamentos_temp 
        MODIFY COLUMN id_paciente INT(11) NOT NULL AFTER id,
        MODIFY COLUMN id_profissional INT(11) NOT NULL AFTER id_paciente,
        MODIFY COLUMN dh_inicio DATETIME NOT NULL AFTER id_profissional,
        MODIFY COLUMN dh_fim DATETIME NOT NULL AFTER dh_inicio,
        MODIFY COLUMN id_convenio INT(11) NOT NULL AFTER dh_fim,
        MODIFY COLUMN id_procedimento VARCHAR(255) NOT NULL AFTER id_convenio,
        MODIFY COLUMN observacoes TEXT AFTER id_procedimento;"
];

foreach ($alterTablesQueriesTemp as $query) {
    executeQuery($connTemp, $query, "Alterações na tabela temporária realizadas com sucesso.", "Erro ao alterar tabelas temporárias");
}

// Atualização de valores nas tabelas temporárias
$updateQueries = [
    "UPDATE pacientes_temp 
SET 
    id_convenio = CASE 
            WHEN id_convenio = 2 THEN 1
            WHEN id_convenio = 5 THEN 2
            WHEN id_convenio = 3 THEN 4
            ELSE id_convenio END,
            id = CASE
            WHEN id = 10272 THEN 2
            WHEN id = 10276 THEN 3
            ELSE id END;",

    "UPDATE agendamentos_temp
SET 
    id_profissional = CASE 
        WHEN id_profissional = 1 THEN 85218
        WHEN id_profissional = 2 THEN 85217
        ELSE id_profissional END,
    id_procedimento = CASE 
        WHEN id_procedimento = 'Consulta' THEN 1
        WHEN id_procedimento = 'retorno' THEN 2
        WHEN id_procedimento = 'acompanhamento' THEN 3
        ELSE id_procedimento END,
    id_convenio = CASE
        WHEN id_convenio = 2 THEN 1
        WHEN id_convenio = 5 THEN 2
        WHEN id_convenio = 3 THEN 4
        ELSE id_convenio END
WHERE id IS NOT NULL;
"
];

foreach ($updateQueries as $query) {
    executeQuery($connTemp, $query, "Atualização de valores realizada com sucesso.", "Erro ao atualizar valores");
}

// Criação do dump das tabelas temporárias
$dumpFile = 'C:/Users/lucas.prigol/Desktop/migration-challenge-main/dump_temp.sql';
$dumpCommand = 'mysqldump -hlocalhost -uroot -p123456 banco_temporario pacientes_temp agendamentos_temp > "' . $dumpFile . '"';
exec($dumpCommand, $output, $returnVar);

if ($returnVar === 0) {
    // Adicionar comando USE ao dump
    $databaseName = 'MedicalChallenge';
    $dumpWithUseFile = 'C:/Users/lucas.prigol/Desktop/migration-challenge-main/dump_temp_with_use.sql';
    $dumpContent = file_get_contents($dumpFile);
    $useCommand = "USE $databaseName;\n";
    file_put_contents($dumpWithUseFile, $useCommand . $dumpContent);
    
    echo "Arquivo de dump atualizado com o comando USE. Arquivo: $dumpWithUseFile<br>";
    

    replaceCollation($dumpWithUseFile, 'utf8mb4_0900_ai_ci', 'utf8mb4_general_ci');

    // Importar o arquivo dump para o banco de dados Medicalchallenge
    $importCommand = 'mysql -hlocalhost -uroot -proot -P 3307 MedicalChallenge < "' . $dumpWithUseFile . '" 2>&1';
    $importOutput = shell_exec($importCommand);

    if ($importOutput === NULL) {
        echo "Importação realizada com sucesso.";
    } else {
        echo "Erro ao importar o dump para o banco de dados destino:<br>" . nl2br(htmlspecialchars($importOutput));
    }

} else {
    echo "Erro ao realizar o dump: " . implode("\n", $output);
}

// Inserir dados da tabela pacientes_temp para a tabela pacientes
$queryPacientes = "
    INSERT INTO pacientes (id, nome, sexo, nascimento, cpf, rg, id_convenio, cod_referencia)
    SELECT id, nome, sexo, nascimento, cpf, rg, id_convenio, cod_referencia
    FROM pacientes_temp
    ON DUPLICATE KEY UPDATE
        nome = VALUES(nome),
        sexo = VALUES(sexo),
        nascimento = VALUES(nascimento),
        cpf = VALUES(cpf),
        rg = VALUES(rg),
        id_convenio = VALUES(id_convenio),
        cod_referencia = VALUES(cod_referencia);
";


executeQuery($connMedical, $queryPacientes, "Dados da tabela pacientes_temp inseridos com sucesso na tabela pacientes.", "Erro ao inserir dados na tabela pacientes");

// Inserir dados da tabela agendamentos_temp para a tabela agendamentos
$queryAgendamentos = "
    INSERT INTO agendamentos (id, id_paciente, id_profissional, dh_inicio, dh_fim, id_convenio, id_procedimento, observacoes)
    SELECT id, id_paciente, id_profissional, dh_inicio, dh_fim, id_convenio, id_procedimento, observacoes
    FROM agendamentos_temp
    ON DUPLICATE KEY UPDATE
        id_paciente = VALUES(id_paciente),
        id_profissional = VALUES(id_profissional),
        dh_inicio = VALUES(dh_inicio),
        dh_fim = VALUES(dh_fim),
        id_convenio = VALUES(id_convenio),
        id_procedimento = VALUES(id_procedimento),
        observacoes = VALUES(observacoes);
";

executeQuery($connMedical, $queryAgendamentos, "Dados da tabela agendamentos_temp inseridos com sucesso na tabela agendamentos.", "Erro ao inserir dados na tabela agendamentos");

// Exclusão das tabelas temporárias do banco de dados MedicalChallenge
$dropTablesQueries = [
    "DROP TABLE IF EXISTS pacientes_temp;",
    "DROP TABLE IF EXISTS agendamentos_temp;"
];

foreach ($dropTablesQueries as $query) {
    executeQuery($connMedical, $query, "Tabela temporária excluída com sucesso.", "Erro ao excluir tabela temporária");
}

// Fechar as conexões
mysqli_close($connTemp);
mysqli_close($connMedical);

// Informações de Fim da Migração:
    echo "Fim da Migração: " . dateNow() . ".\n";
?>
