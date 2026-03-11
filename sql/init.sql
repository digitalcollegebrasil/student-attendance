CREATE TABLE frequencia (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

    data_aula DATE NOT NULL,
    turma VARCHAR(255) NOT NULL,
    curso VARCHAR(150),
    professor VARCHAR(150),

    vagas INT,
    integrantes INT,
    trancados INT,

    horario VARCHAR(100),
    nao_frequente INT,
    frequente INT,

    dias_semana VARCHAR(100),
    sede VARCHAR(50),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX uq_frequencia_data_turma
    ON frequencia (data_aula, turma);