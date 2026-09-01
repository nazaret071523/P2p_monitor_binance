import os
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()


def _conexion():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL no está configurada")
    return psycopg2.connect(DATABASE_URL, connect_timeout=10)


def registrar_pago_db(telegram_id: int, username: str, referencia: str) -> bool:
    """Registra o actualiza un pago como pendiente."""
    try:
        with _conexion() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO usuarios_p2p
                        (telegram_id, username, estado_suscripcion, referencia_pago, actualizado_en)
                    VALUES (%s, %s, 'pendiente', %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (telegram_id)
                    DO UPDATE SET
                        username = EXCLUDED.username,
                        referencia_pago = EXCLUDED.referencia_pago,
                        estado_suscripcion = 'pendiente',
                        actualizado_en = CURRENT_TIMESTAMP;
                """, (telegram_id, username, referencia))
        return True
    except Exception as e:
        print(f"Error en registrar_pago_db: {e}")
        return False


def verificar_estado_usuario(telegram_id: int) -> dict:
    """Devuelve el estado de la suscripción y marca vencida si ya expiró."""
    try:
        with _conexion() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE usuarios_p2p
                    SET estado_suscripcion = 'vencido',
                        actualizado_en = CURRENT_TIMESTAMP
                    WHERE telegram_id = %s
                      AND estado_suscripcion = 'activo'
                      AND fecha_expiracion IS NOT NULL
                      AND fecha_expiracion < CURRENT_TIMESTAMP;
                """, (telegram_id,))

                cur.execute("""
                    SELECT estado_suscripcion, fecha_expiracion, referencia_pago
                    FROM usuarios_p2p
                    WHERE telegram_id = %s;
                """, (telegram_id,))
                row = cur.fetchone()

        if row:
            return {
                "estado": row[0],
                "expiracion": row[1],
                "referencia": row[2],
            }
        return {"estado": "no_registrado"}
    except Exception as e:
        print(f"Error en verificar_estado_usuario: {e}")
        return {"estado": "error"}


def aprobar_usuario_db(telegram_id: int) -> bool:
    """Activa o renueva la suscripción por 30 días."""
    try:
        with _conexion() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE usuarios_p2p
                    SET estado_suscripcion = 'activo',
                        fecha_expiracion = CURRENT_TIMESTAMP + INTERVAL '30 days',
                        actualizado_en = CURRENT_TIMESTAMP
                    WHERE telegram_id = %s;
                """, (telegram_id,))
                actualizado = cur.rowcount > 0
        return actualizado
    except Exception as e:
        print(f"Error en aprobar_usuario_db: {e}")
        return False
