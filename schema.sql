-- VENBOT / SUPABASE
-- Ejecuta este script una sola vez en Supabase > SQL Editor si prefieres
-- crear/verificar las tablas manualmente. bot.py también intenta crearlas.

CREATE TABLE IF NOT EXISTS muestras_p2p (
    id BIGSERIAL PRIMARY KEY,
    compra DOUBLE PRECISION NOT NULL,
    venta DOUBLE PRECISION NOT NULL,
    liquidez_score INTEGER DEFAULT 0,
    banco TEXT DEFAULT 'GENERAL',
    fecha TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_muestras_p2p_banco_fecha
ON muestras_p2p (banco, fecha DESC);

CREATE TABLE IF NOT EXISTS mercado_actual (
    id INTEGER PRIMARY KEY,
    compra DOUBLE PRECISION NOT NULL,
    venta DOUBLE PRECISION NOT NULL,
    liquidez_score INTEGER DEFAULT 0,
    bcv_usd DOUBLE PRECISION,
    bcv_eur DOUBLE PRECISION,
    fuente_bcv TEXT,
    fecha TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS usuarios_p2p (
    telegram_id BIGINT PRIMARY KEY,
    username TEXT,
    estado_suscripcion TEXT DEFAULT 'no_registrado',
    referencia_pago TEXT,
    fecha_expiracion TIMESTAMPTZ,
    creado_en TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    actualizado_en TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
