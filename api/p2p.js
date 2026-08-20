export default async function handler(req, res) {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET');

    try {
        const [resSell, resBuy] = await Promise.all([
            fetch("https://criptoya.com/api/binancep2p/sell/usdt/ves/5"),
            fetch("https://criptoya.com/api/binancep2p/buy/usdt/ves/5")
        ]);

        const comprar = await resSell.json();
        const vender = await resBuy.json();

        return res.status(200).json({ comprar, vender });
    } catch (error) {
        return res.status(500).json({ error: "Error obteniendo datos de Binance P2P" });
    }
}
