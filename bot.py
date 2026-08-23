def consultar_binance_top1(trade_type, monto):
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    payload = json.dumps({
        "asset": "USDT",
        "fiat": "VES",
        "merchantCheck": False,  # Desactiva la verificación exclusiva de Merchants
        "publisherType": None,   # Trae todos los anuncios (incluyendo usuarios comunes)
        "page": 1,
        "rows": 10,
        "tradeType": trade_type,
        "transAmount": str(monto)
    }).encode('utf-8')
    
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    
    try:
        req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=6) as response:
            res = json.loads(response.read().decode('utf-8'))
            data = res.get('data', [])
            
            # Filtramos localmente para garantizar tomar solo usuarios NO verificados
            for item in data:
                advertiser = item.get('advertiser', {})
                user_type = advertiser.get('userType', '')
                
                # 'user' es usuario normal/no verificado. 'merchant' es verificado.
                if user_type != 'merchant':
                    return round(float(item['adv']['price']), 2)
                    
            # Si no hay usuarios comunes en los primeros 10, toma el primer anuncio disponible
            if data:
                return round(float(data[0]['adv']['price']), 2)
    except Exception as e:
        print(f"Error consultando Binance Top1: {e}")
    return None
