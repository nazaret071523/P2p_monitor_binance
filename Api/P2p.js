const fetch = require('node-fetch');

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');

  const fetchTrade = async (type) => {
    const url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search";
    const payload = {
      asset: "USDT",
      fiat: "VES",
      merchantCheck: false,
      page: 1,
      rows: 10,
      tradeType: type
    };

    try {
      const response = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
          "Accept": "*/*",
          "Cache-Control": "no-cache"
        },
        body: JSON.stringify(payload)
      });

      const data = await response.json();
      return data.data || [];
    } catch (err) {
      return [];
    }
  };

  try {
    const [buys, sells] = await Promise.all([fetchTrade("BUY"), fetchTrade("SELL")]);
    return res.status(200).json({ status: "success", buy: buys, sell: sells });
  } catch (error) {
    return res.status(500).json({ status: "error", message: error.message });
  }
};
