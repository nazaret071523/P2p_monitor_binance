const fetch = require('node-fetch');

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');

  try {
    const responseSell = await fetch("https://criptoya.com/api/binancep2p/sell/usdt/ves/5", {
      headers: { "User-Agent": "Mozilla/5.0" }
    });
    
    const responseBuy = await fetch("https://criptoya.com/api/binancep2p/buy/usdt/ves/5", {
      headers: { "User-Agent": "Mozilla/5.0" }
    });

    const sellData = await responseSell.json();
    const buyData = await responseBuy.json();

    const buys = Object.values(sellData).slice(0, 5).map(item => ({
      adv: { price: item.price },
      advertiser: { nickName: item.userName || "P2P User" }
    }));

    const sells = Object.values(buyData).slice(0, 5).map(item => ({
      adv: { price: item.price },
      advertiser: { nickName: item.userName || "P2P User" }
    }));

    return res.status(200).json({ status: "success", buy: buys, sell: sells });
  } catch (error) {
    return res.status(500).json({ status: "error", message: error.message });
  }
};
