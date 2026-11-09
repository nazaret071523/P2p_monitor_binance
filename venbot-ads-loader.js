/* VENBOT AdSense loader: FREE only, default-off, and consent-signal gated. */
(function () {
  'use strict';
  const cfg = window.VENBOT_AD_CONFIG || {};
  const placements = ['header', 'top', 'footer'];
  let adsScriptPromise = null;
  let cmpReadyPromise = null;

  function validPublisher(value) {
    return /^ca-pub-\d{10,25}$/.test(String(value || ''));
  }

  function configuredSlots() {
    const result = {};
    placements.forEach((name) => {
      const slot = String(cfg.slots?.[name] || '').trim();
      if (/^\d{6,25}$/.test(slot)) result[name] = slot;
    });
    return result;
  }

  function isReady() {
    return Boolean(
      cfg.webEnabled === true &&
      cfg.siteApproved === true &&
      cfg.cmpConfigured === true &&
      validPublisher(cfg.publisherId) &&
      Object.keys(configuredSlots()).length > 0 &&
      typeof window.__tcfapi === 'function'
    );
  }

  // Existence of __tcfapi is not proof that the CMP has loaded a valid user
  // choice. Wait for a TCF lifecycle event before loading Google's ad library.
  // Google reads the TCF signal itself; Venbot does not override consent choices.
  function waitForCmpSignal(timeoutMs = 3000) {
    if (cmpReadyPromise) return cmpReadyPromise;
    if (typeof window.__tcfapi !== 'function') return Promise.resolve(false);
    cmpReadyPromise = new Promise((resolve) => {
      let settled = false;
      let timer = null;
      const finish = (ready) => {
        if (settled) return;
        settled = true;
        if (timer !== null) window.clearTimeout(timer);
        if (!ready) cmpReadyPromise = null;
        resolve(Boolean(ready));
      };
      timer = window.setTimeout(() => finish(false), timeoutMs);
      const callback = (tcData, success) => {
        if (!success || !tcData) return finish(false);
        if (tcData.cmpStatus === 'error') return finish(false);
        if (tcData.cmpStatus !== 'loaded') return;
        if (tcData.eventStatus !== 'tcloaded' && tcData.eventStatus !== 'useractioncomplete') return;
        if (tcData.gdprApplies === true && !String(tcData.tcString || '').trim()) return finish(false);
        finish(true);
      };
      try {
        window.__tcfapi('addEventListener', 2, callback);
      } catch (_error) {
        finish(false);
      }
    });
    return cmpReadyPromise;
  }

  function loadLibrary() {
    if (window.adsbygoogle) return Promise.resolve();
    if (adsScriptPromise) return adsScriptPromise;
    adsScriptPromise = new Promise((resolve, reject) => {
      const script = document.createElement('script');
      script.async = true;
      script.crossOrigin = 'anonymous';
      script.src = 'https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js?client=' + encodeURIComponent(cfg.publisherId);
      script.onload = () => resolve();
      script.onerror = () => {
        adsScriptPromise = null;
        reject(new Error('No se pudo cargar AdSense'));
      };
      document.head.appendChild(script);
    });
    return adsScriptPromise;
  }

  async function sync(plan) {
    const normalized = String(plan || 'FREE').toUpperCase();
    if (normalized !== 'FREE' || !isReady()) return false;
    const consentSignalReady = await waitForCmpSignal();
    if (!consentSignalReady) return false;
    const slots = configuredSlots();
    try {
      await loadLibrary();
      document.querySelectorAll('.ad-slot[data-ad-placement]').forEach((host) => {
        const placement = host.getAttribute('data-ad-placement');
        const slot = slots[placement === 'banner-top' ? 'top' : placement];
        if (!slot || host.dataset.venbotAdLoaded === 'true') return;
        const ins = document.createElement('ins');
        ins.className = 'adsbygoogle';
        ins.style.display = 'block';
        ins.style.minHeight = '90px';
        ins.setAttribute('data-ad-client', cfg.publisherId);
        ins.setAttribute('data-ad-slot', slot);
        ins.setAttribute('data-ad-format', 'auto');
        ins.setAttribute('data-full-width-responsive', 'true');
        host.replaceChildren(ins);
        host.dataset.venbotAdLoaded = 'true';
        try {
          (window.adsbygoogle = window.adsbygoogle || []).push({});
        } catch (err) {
          host.dataset.venbotAdLoaded = 'false';
          console.warn('Venbot AdSense: unidad no inicializada', err);
        }
      });
      return true;
    } catch (err) {
      console.warn('Venbot AdSense permanece inactivo', err);
      return false;
    }
  }

  window.VenbotAds = Object.freeze({ isReady, sync, waitForCmpSignal });
})();
