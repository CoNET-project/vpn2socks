//  DNSInterceptor.swift
//  vpn2socks
//
//  Integrated with FakeIPAllocator for lazy on-demand fake-IP generation
//  Added intelligent DNS proxy for APNs domain forwarding
//  Created by peter on 2025-08-17. Updated by ChatGPT on 2025-08-21
//

import Foundation
import NetworkExtension
import Network
import os.log

private let logger = Logger(subsystem: "com.vpn2socks", category: "DNSInterceptor")

// MARK: - DNSInterceptor (actor)
final actor DNSInterceptor {
    static let shared = DNSInterceptor()   // ✅ Singleton

    // 映射
    private var fakeIPToDomainMap: [IPv4Address: String] = [:]
    private var domainToFakeIPMap: [String: IPv4Address] = [:]

    // 引用计数（不再触发删除）
    private var holds: [IPv4Address: Int] = [:]

    // 可选回收列表（长期映射默认不回收）
    private var freeList: [IPv4Address] = []

    // ✅ Lazy fake-IP allocator
    private var allocator: FakeIPAllocator

    // ✅ DNS转发相关
    private let upstreamDNSServers = ["8.8.8.8", "1.1.1.1", "208.67.222.222"] // Google, Cloudflare, OpenDNS
    private var dnsCache: [String: DNSCacheEntry] = [:]
    private let dnsCacheTTL: TimeInterval = 300 // 5分钟缓存
    
    // DNS查询超时和重试
    private let dnsQueryTimeout: TimeInterval = 3.0
    private let maxDNSRetries = 2

    // ✅ APNs 域名列表（支持直连的域名）
    private let apnsDomains: Set<String> = [
        "conet.network",
        "silentpass.io",
        "openpgp.online",
        "comm100vue.com",
        "comm100.io",
        "doubleclick.net",
        "googleadservices.com",
        "googlesyndication.com",
        "googletagmanager.com",
        "googletagservices.com",
        "google-analytics.com",
        "googleanalytics.com",
        "adsystem.com",
        "adsrvr.org",
        "onetrust.com",
        "liadm.com",

        // Facebook/Meta
        "facebook-analytics.com",
        "fbcdn.net",

        // Amazon
        "amazontrust.com",

        // Microsoft
        "adsrvr.org",
        "bing.com",
        "msftconnecttest.com",

        // 通用广告网络
        "adsrvr.org",
        "adnxs.com",
        "adzerk.net",
        "pubmatic.com",
        "criteo.com",
        "criteo.net",
        "casalemedia.com",
        "openx.net",
        "rubiconproject.com",
        "serving-sys.com",
        "taboola.com",
        "outbrain.com",
        "media.net",
        "yieldmo.com",
        "3lift.com",
        "indexexchange.com",
        "sovrn.com",
        "sharethrough.com",
        "spotx.tv",
        "springserve.com",
        "tremor.io",
        "tribalfusion.com",
        "undertone.com",
        "yieldlab.net",
        "yieldmanager.com",
        "zedo.com",
        "zemanta.com",

        // 分析和跟踪
        "scorecardresearch.com",
        "quantserve.com",
        "imrworldwide.com",
        "nielsen.com",
        "alexa.com",
        "hotjar.com",
        "mouseflow.com",
        "luckyorange.com",
        "clicktale.com",
        "demdex.net",
        "krxd.net",
        "bluekai.com",
        "exelator.com",
        "mathtag.com",
        "turn.com",
        "acuityplatform.com",
        "adform.net",
        "bidswitch.net",
        "contextweb.com",
        "districtm.io",
        "emxdgt.com",
        "gumgum.com",
        "improve-digital.com",
        "inmobi.com",
        "loopme.com",
        "mobfox.com",
        "nexage.com",
        "rhythmone.com",
        "smaato.com",
        "smartadserver.com",
        "stroeer.io",
        "teads.tv",
        "triplelift.com",
        "verizonmedia.com",
        "vertamedia.com",
        "video.io",
        "viralize.com",
        "weborama.com",
        "widespace.com",

        // 中国广告网络
        
        "tanx.com",
        "mediav.com",
        "admaster.com.cn",
        "dsp.com",
        "vamaker.com",
        "allyes.com",
        "ipinyou.com",
        "irs01.com",
        "istreamsche.com",
        "jusha.com",
        "knet.cn",
        "madserving.com",
        "miaozhen.com",
        "mmstat.com",
        "moad.cn",
        "mobaders.com",
        "mydas.mobi",
        "n.shifen.com",
        "netease.gg",
        "newrelic.com",
        "nexac.com",
        "ntalker.com",
        "nylalobghyhirgh.com",
        "o2omobi.com",
        "oimagea2.ydstatic.com",
        "optaim.com",
        "optimix.asia",
        "optimizely.com",
        "overture.com",
        "p0y.cn",
        "pagead.l.google.com",
        "pageadimg.l.google.com",
        "pbcdn.com",
        "pingdom.net",
        "pixanalytics.com",
        "ppjia55.com",
        "punchbox.org",
        "qchannel01.cn",
        "qiyou.com",
        "qtmojo.com",
        "quantcount.com",

        // 恶意软件和垃圾邮件
        "2o7.net",
        "omtrdc.net",
        "everesttech.net",
        "everest-tech.net",
        "rubiconproject.com",
        "adsafeprotected.com",
        "adsymptotic.com",
        "adtechjp.com",
        "advertising.com",
        "evidon.com",
        "voicefive.com",
        "buysellads.com",
        "carbonads.com",
        "zdbb.net",
        "trackcmp.net",
        

        // 更多跟踪器
        "mixpanel.com",
        "kissmetrics.com",
        "segment.com",
        "segment.io",
        "keen.io",
        "amplitude.com",
        "appsflyer.com",
        "branch.io",
        "adjust.com",
        "kochava.com",
        "tenjin.io",
        "singular.net",
        "apptentive.com",
        "appboy.com",
        "braze.com",
        "customer.io",
        "intercom.io",
        "drift.com",
        "zendesk.com",
        // Apple Push 相关
        "conet.network",
        "apple.com",
        "push.apple.com",
        "cdn-apple.com",
        "cdnst.net",
        "icloud.com",
        "push-apple.com.akadns.net",
        "amazon-adsystem.com",
        "silentpass.io",
        "ziffstatic.com",
        "cdn.ziffstatic.com",
        "courier.push.apple.com",
        "gateway.push.apple.com",
        "gateway.sandbox.push.apple.com",
        "gateway.icloud.com",
        "bag.itunes.apple.com",
        "init.itunes.apple.com",
        "xp.apple.com",
		"icloud-content.com",
        "gsa.apple.com",
        "gsp-ssl.ls.apple.com",
        "gsp-ssl.ls-apple.com.akadns.net",
        "mesu.apple.com",
        "gdmf.apple.com",
        "deviceenrollment.apple.com",
        "mdmenrollment.apple.com",
        "iprofiles.apple.com",
        "ppq.apple.com",
        "baidu.com",
        "bdstatic.com",

        // 🔥 微信（WeChat）相关域名
        "wechat.com",
        "weixin.qq.com",
        "weixin110.qq.com",
        "tenpay.com",
        "mm.taobao.com",
        "wx.qq.com",
        "web.wechat.com",
        "webpush.weixin.qq.com",
        "qpic.cn",
        "qlogo.cn",
        "wx.gtimg.com",
        "minorshort.weixin.qq.com",
        "log.weixin.qq.com",
        "szshort.weixin.qq.com",
        "szminorshort.weixin.qq.com",
        "szextshort.weixin.qq.com",
        "hkshort.weixin.qq.com",
        "hkminorshort.weixin.qq.com",
        "hkextshort.weixin.qq.com",
        "hklong.weixin.qq.com",
        "sgshort.wechat.com",
        "sgminorshort.wechat.com",
        "sglong.wechat.com",
        "usshort.wechat.com",
        "usminorshort.wechat.com",
        "uslong.wechat.com",

        // 微信支付
        "pay.weixin.qq.com",
        "payapp.weixin.qq.com",

        // 微信文件传输
        "file.wx.qq.com",
        "support.weixin.qq.com",

        // 微信 CDN
        "mmbiz.qpic.cn",
        "mmbiz.qlogo.cn",
        "mmsns.qpic.cn",
        "sync.com",

        // 腾讯推送服务
        "dns.weixin.qq.com",
        "short.weixin.qq.com",
        "long.weixin.qq.com",

        "doubleclick.net",
        "pubmatic.com",
        "adnxs.com",
        "rubiconproject.com",

        "adsrvr.org",
        "criteo.com",

        "taboola.com",
        "yahoo.com",
        "publicsuffix.org",
		"amazonaws.com"
            
    ]

    // 持久化
    private static let persistKey = "DNSInterceptor.FakeIPMap.v2"

    private struct PersistBlob: Codable {
        var mapping: [String:String] // ipString -> domain
        var rangeIndex: Int
        var nextIP: UInt32
    }
    
    private var lastAPNsResolveAt: Date = .distantPast

    // ✅ DNS缓存条目
    private struct DNSCacheEntry {
        let response: Data
        let timestamp: Date
        let ttl: TimeInterval
        
        var isExpired: Bool {
            Date().timeIntervalSince(timestamp) > ttl
        }
    }

    // MARK: - 静态读取（供 init 使用）
    private static func loadPersistedBlob() -> PersistBlob? {
        guard let data = UserDefaults.standard.data(forKey: Self.persistKey) else { return nil }
        return try? JSONDecoder().decode(PersistBlob.self, from: data)
    }

    private init() {
        // Build ranges based on your deployment needs.
        let rfc2544 = IPv4Range(cidr: "198.18.0.0/15")!
        let ranges = [rfc2544]

        let reserved: Set<UInt32> = [IPv4Address("198.18.0.0")!.u32,
                                     IPv4Address("198.19.255.255")!.u32]

        var alloc = FakeIPAllocator(ranges: ranges, reserved: reserved)

        // Restore mappings and allocator cursor if available
        if let blob = Self.loadPersistedBlob() {
            var ipToDomain: [IPv4Address:String] = [:]
            var domainToIP: [String:IPv4Address] = [:]
            for (ipStr, dom) in blob.mapping {
                if let ip = IPv4Address(ipStr) {
                    let d = Self.normalize(dom)
                    ipToDomain[ip] = d
                    domainToIP[d] = ip
                }
            }
            self.fakeIPToDomainMap = ipToDomain
            self.domainToFakeIPMap  = domainToIP
            alloc.restoreCursor(rangeIndex: blob.rangeIndex, nextIP: blob.nextIP)
            logger.info("Restored \(ipToDomain.count) mappings, cursor=(range=\(blob.rangeIndex), nextIP=\(blob.nextIP)))")
        }

        self.allocator = alloc
        logger.info("Initialized with lazy FakeIPAllocator (RFC2544 /15) + intelligent DNS proxy")
        
        // ✅ 修复：使用 Task 包装异步调用
        Task { [weak self] in
            await self?.startCacheCleanupTask()
        }
    }

    // 统一域名归一化
    @inline(__always)
    private static func normalize(_ domain: String) -> String {
        let trimmed = domain.trimmingCharacters(in: .whitespacesAndNewlines)
        let noDot = trimmed.hasSuffix(".") ? String(trimmed.dropLast()) : trimmed
        if !noDot.isEmpty {
            var comps = URLComponents()
            comps.scheme = "https"
            comps.host = noDot
            if let h1 = comps.percentEncodedHost, !h1.isEmpty { return h1.lowercased() }
            if let h2 = comps.host, !h2.isEmpty { return h2.lowercased() }
        }
        return noDot.lowercased()
    }

    // ✅ 检测域名是否应该直连（APNs 域名）
    private func shouldBypassTunnel(_ domain: String) -> Bool {
        let normalized = Self.normalize(domain)
        
        // 精确匹配
        if apnsDomains.contains(normalized) {
            return true
        }
        
        // 后缀匹配（支持子域名）
        for apnsDomain in apnsDomains {
            if normalized.hasSuffix("." + apnsDomain) {
                return true
            }
        }
        
        // 通配符匹配 Apple 推送相关域名
        if normalized.contains("push.apple.com") ||
           normalized.contains("gateway.apple.com") ||
           normalized.hasSuffix(".push.apple.com") {
            return true
        }
        
        return false
    }

    // MARK: - API

    /// 为域名分配或取回假 IP
    func allocOrGet(for domain: String) -> IPv4Address? {
        let d = Self.normalize(domain)
        
        // ✅ 检查是否应该绕过隧道
        if shouldBypassTunnel(d) {
            logger.info("Bypassing tunnel for APNs domain: \(d)")
            
            // 清理可能存在的旧映射
            if let existingFakeIP = domainToFakeIPMap[d] {
                domainToFakeIPMap.removeValue(forKey: d)
                fakeIPToDomainMap.removeValue(forKey: existingFakeIP)
                logger.info("Removed stale fake IP mapping for APNs domain: \(d)")
                persistNow()
            }
            
            return nil  // 返回 nil 表示不分配假 IP，应该直连
        }
        
        if let ip = domainToFakeIPMap[d] { return ip }
        return allocateFakeIP(for: d)
    }

    /// 通过假 IP 反查域名
    func lookupDomain(by ip: IPv4Address) -> String? {
        fakeIPToDomainMap[ip]
    }

    /// 判定是否属于 198.18.0.0/15 假 IP
    func contains(_ ip: IPv4Address) -> Bool {
        let o = ip.rawValue
        return o.count == 4 && o[0] == 198 && (o[1] == 18 || o[1] == 19)
    }

    /// 引用计数 +1
    func retain(fakeIP: IPv4Address) { holds[fakeIP, default: 0] += 1 }

    /// 引用计数 -1
    func release(fakeIP: IPv4Address) {
        if let c = holds[fakeIP], c > 1 {
            holds[fakeIP] = c - 1
        } else {
            holds.removeValue(forKey: fakeIP)
        }
    }

    func registerMapping(fakeIP: IPv4Address, domain: String) {
        let d = Self.normalize(domain)
        if let existed = fakeIPToDomainMap[fakeIP], existed != d {
            logger.warning("Overwrite mapping \(String(describing: fakeIP)) : \(existed) -> \(d)")
        }
        fakeIPToDomainMap[fakeIP] = d
        domainToFakeIPMap[d] = fakeIP
        persistNow()
    }

    func getDomain(forFakeIP ip: IPv4Address) -> String? { fakeIPToDomainMap[ip] }

    // MARK: - DNS 处理 (改进版本)

    func handleQueryAndCreateResponse(for queryData: Data) async -> (response: Data, fakeIP: IPv4Address?)? {
        guard let qname = extractDomainName(from: queryData) else { return nil }
        let domain = Self.normalize(qname)
        let qtype = extractQType(from: queryData) ?? 0

        // ✅ 检查是否应该绕过隧道 - 使用智能转发
        if shouldBypassTunnel(domain) {
            logger.info("DNS forwarding for APNs domain: \(domain)")
            // 🔑 关键改进：转发到上游DNS而不是返回NXDOMAIN
            return await forwardToUpstreamDNS(queryData: queryData, domain: domain, qtype: qtype)
        }

        // 普通域名的处理逻辑
        if qtype == 1 {
            guard let fakeIP = allocOrGet(for: domain) else { return nil }
            let resp = createDNSResponse(for: queryData, ipAddress: fakeIP)
            logger.debug("Created A record response for \(domain) -> \(String(describing: fakeIP))")
            return (resp, fakeIP)
        } else {
            _ = allocOrGet(for: domain)
            let resp = createNoDataSOAResponse(for: queryData, negativeTTL: 10)
            logger.debug("Non-A query \(domain), qtype=\(qtype) -> NODATA + SOA(10s)")
            return (resp, nil)
        }
    }

    
    // 提供最近一次 APNs 直连域名解析的时间，用于健康检测
    func getLastAPNsResolveAt() -> Date { lastAPNsResolveAt }
    // MARK: - 智能DNS转发实现

    /// ✅ 核心功能：转发DNS查询到上游服务器
    private func forwardToUpstreamDNS(queryData: Data, domain: String, qtype: UInt16) async -> (response: Data, fakeIP: IPv4Address?)? {
        // 检查缓存
        let cacheKey = "\(domain):\(qtype)"
        if let cached = dnsCache[cacheKey], !cached.isExpired {
            logger.debug("DNS cache hit for \(domain)")
            return (cached.response, nil)
        }
        
        // 如果只处理A记录查询
        guard qtype == 1 else {
            logger.debug("Non-A query for APNs domain \(domain), returning NXDOMAIN")
            return (createNXDomainResponse(for: queryData), nil)
        }
        
        // 尝试从上游DNS服务器获取真实IP
        for (index, dnsServer) in upstreamDNSServers.enumerated() {
            logger.debug("Querying upstream DNS \(dnsServer) for \(domain) (attempt \(index + 1))")
            
            if let response = await queryUpstreamDNS(queryData: queryData, serverIP: dnsServer, domain: domain) {
                // 缓存成功的响应
                let cacheEntry = DNSCacheEntry(response: response, timestamp: Date(), ttl: dnsCacheTTL)
                dnsCache[cacheKey] = cacheEntry
                
                logger.info("Successfully resolved \(domain) via upstream DNS \(dnsServer)")
                self.lastAPNsResolveAt = Date()
                return (response, nil)
            }
        }
        
        // 所有上游DNS都失败，返回临时失败
        logger.error("All upstream DNS servers failed for \(domain)")
        return (createTempFailResponse(for: queryData), nil)
    }

	/// 并发向所有上游 DoH 发送查询，最先成功的结果即返回；同时按响应中的 TTL 写入本地缓存
    private func forwardToUpstreamDNS(_ dnsQuery: Data, domain: String) async -> Data? {
        // 并发竞速
        let servers = upstreamDNSServers
        return await withTaskGroup(of: Data?.self) { group in
            // 可选的轻微优先级：让 1.1.1.1 先发起（更快首包），其他紧随其后
            for (idx, ip) in servers.enumerated() {
                group.addTask { [weak self] in
                    if idx > 0 { try? await Task.sleep(nanoseconds: 30_000_000) } // 30ms 微延迟
                    return await self?.queryUpstreamDNS(queryData: dnsQuery, serverIP: ip, domain: domain)
                }
            }
            var winner: Data? = nil
            for await result in group {
                if let r = result {
                    winner = r
                    group.cancelAll()
                    break
                }
            }
            // 写缓存（若能解析到 TTL）
            if let resp = winner {
                let ttl = extractMinTTL(from: resp) ?? dnsCacheTTL
                cache(domain: domain, responseData: resp, ttl: ttl)
            }
            return winner
        }
    }

    // MARK: - DoH helpers + TTL 解析
    /// 将上游 IP 映射到常见 DoH 端点，默认使用 https://<ip>/dns-query
    private func dohURL(for s: String) -> URL? {
        if s == "8.8.8.8" || s == "8.8.4.4" { return URL(string: "https://dns.google/dns-query") }
        if s == "1.1.1.1" || s == "1.0.0.1" { return URL(string: "https://1.1.1.1/dns-query") }
        if s == "9.9.9.9" { return URL(string: "https://dns.quad9.net/dns-query") }
        return URL(string: "https://\(s)/dns-query")
    }

	/// 解析最小 TTL（秒）。简单 DNS parser：跳过 Questions，读取 Answers 的 TTL，取最小值
    private func extractMinTTL(from dns: Data) -> TimeInterval? {
        // 最低头长度 12 字节
        guard dns.count >= 12 else { return nil }
        func u16(_ i: Int) -> Int { Int(dns[i]) << 8 | Int(dns[i+1]) }
        let qd = u16(4)
        let an = u16(6)
        // 跳过 question 区
		var off = 12
		func skipName(_ start: Int) -> Int? {
			var i = start
			var safety = 0
			while i < dns.count && safety < 256 {
				let len = Int(dns[i])
				if len == 0 { return i + 1 }
				if (len & 0xC0) == 0xC0 { // 压缩指针
					guard i + 1 < dns.count else { return nil }
					return i + 2
				}
				i += 1 + len
				safety += 1
			}
			return nil
		}
		for _ in 0..<qd {
			guard let nameEnd = skipName(off) else { return nil }
			off = nameEnd + 4 // type(2) + class(2)
			if off > dns.count { return nil }
		}
		var minTTL: UInt32?
		for _ in 0..<an {
			guard let nameEnd = skipName(off) else { return minTTL.map { TimeInterval($0) } }
			var p = nameEnd
			guard p + 10 <= dns.count else { break } // type2+class2+ttl4+rdlen2
			// 跳过 type,class
			p += 4
			// 读取 TTL
			let ttl = (UInt32(dns[p]) << 24) | (UInt32(dns[p+1]) << 16) | (UInt32(dns[p+2]) << 8) | UInt32(dns[p+3])
			p += 4
			// 跳过 RDLENGTH + RDATA
			let rdlen = (Int(dns[p]) << 8) | Int(dns[p+1])
			p += 2
			off = p + rdlen
			minTTL = minTTL.map { min($0, ttl) } ?? ttl
			if off > dns.count { break }
		}
		return minTTL.map { TimeInterval($0) }
	}

	// 写入缓存：以 "domain:qtype" 为 key，ttl 为上游答复中的最小 TTL
	private func cache(domain: String, responseData: Data, ttl: TimeInterval) {
		let qtype = extractQTypeFromResponse(responseData) ?? 1 /* A 记录兜底 */
		let key = "\(domain):\(qtype)"
		dnsCache[key] = DNSCacheEntry(response: responseData, timestamp: Date(), ttl: ttl)
	}

	// 从 DNS 报文（请求或响应）中解析第一个 Question 的 qtype
	private func extractQTypeFromResponse(_ dns: Data) -> UInt16? {
		guard dns.count >= 12 else { return nil }
		// 跳过第一个 QNAME，然后读取 2 字节 QTYPE
		func skipName(_ start: Int) -> Int? {
			var i = start
			var safety = 0
			while i < dns.count && safety < 256 {
				let len = Int(dns[i])
				if len == 0 { return i + 1 }
				if (len & 0xC0) == 0xC0 {
					guard i + 1 < dns.count else { return nil }
					return i + 2
				}
				i += 1 + len
				safety += 1
			}
			return nil
		}
		// Questions 计数
		let qd = (Int(dns[4]) << 8) | Int(dns[5])
		guard qd > 0 else { return nil }
		guard let nameEnd = skipName(12) else { return nil }
		guard nameEnd + 2 <= dns.count else { return nil }
		let qtype = (UInt16(dns[nameEnd]) << 8) | UInt16(dns[nameEnd + 1])
		return qtype
	}


    /// DoH：POST application/dns-message
    private func queryUpstreamDNS(queryData: Data, serverIP: String, domain: String) async -> Data? {
        guard let url = dohURL(for: serverIP) else { return nil }
        var req = URLRequest(url: url)
        req.httpMethod = "POST"
        req.setValue("application/dns-message", forHTTPHeaderField: "Content-Type")
        req.setValue("application/dns-message", forHTTPHeaderField: "Accept")
        req.httpBody = queryData

        let cfg = URLSessionConfiguration.ephemeral
        cfg.waitsForConnectivity = true
        cfg.timeoutIntervalForRequest = dnsQueryTimeout
        cfg.timeoutIntervalForResource = dnsQueryTimeout

        do {
            let (data, resp) = try await URLSession(configuration: cfg).data(for: req)
            guard let http = resp as? HTTPURLResponse, http.statusCode == 200 else {
                return nil
            }
            return data
        } catch {
            logger.error("DoH request failed for \(domain): \(error.localizedDescription)")
            return nil
        }
    }

    // MARK: - 缓存管理

    /// 启动缓存清理任务
    private func startCacheCleanupTask() {
        Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 60_000_000_000) // 每分钟清理一次
                await self?.cleanupExpiredCache()
            }
        }
    }

    /// 清理过期的DNS缓存
    private func cleanupExpiredCache() {
        let beforeCount = dnsCache.count
        dnsCache = dnsCache.filter { !$0.value.isExpired }
        let afterCount = dnsCache.count
        
        if beforeCount != afterCount {
            logger.debug("Cleaned up \(beforeCount - afterCount) expired DNS cache entries")
        }
    }

    // MARK: - 内部分配/持久化

    private func allocateFakeIP(for domain: String) -> IPv4Address? {
        if let existing = domainToFakeIPMap[domain] { return existing }
        
        if let reused = freeList.popLast() {
            fakeIPToDomainMap[reused] = domain
            domainToFakeIPMap[domain] = reused
            logger.debug("Reused fake IP \(String(describing: reused)) for domain \(domain)")
            persistNow()
            return reused
        }
        
        var a = allocator
        guard let ip = a.allocate() else {
            logger.error("Fake IP pool exhausted!")
            return nil
        }
        allocator = a
        fakeIPToDomainMap[ip] = domain
        domainToFakeIPMap[domain] = ip
        logger.debug("Allocated fake IP \(String(describing: ip)) for domain \(domain)")
        persistNow()
        return ip
    }

    private func persistNow() {
        var dict: [String:String] = [:]
        dict.reserveCapacity(fakeIPToDomainMap.count)
        for (ip, d) in fakeIPToDomainMap { dict[ip.debugDescription] = d }
        let snap = allocator.snapshotCursor()
        let blob = PersistBlob(mapping: dict, rangeIndex: snap.rangeIndex, nextIP: snap.nextIP)
        if let data = try? JSONEncoder().encode(blob) {
            UserDefaults.standard.set(data, forKey: Self.persistKey)
        }
    }

    // MARK: - DNS response builders

    /// 创建临时失败响应 (SERVFAIL)
    private func createTempFailResponse(for queryData: Data) -> Data {
        guard queryData.count >= 12 else { return Data() }
        var resp = Data(queryData.prefix(12))
        
        // 设置响应标志
        resp[2] = 0x80 | (queryData[2] & 0x01) // QR=1, RD保持
        resp[3] = 0x82 // SERVFAIL (RCODE=2)
        
        // 复制问题段
        if queryData.count > 12 {
            resp.append(queryData[12...])
        }
        
        return resp
    }

    /// 创建 NXDOMAIN 响应
    private func createNXDomainResponse(for queryData: Data) -> Data {
        guard queryData.count >= 12 else { return Data() }
        var idx = 12
        while idx < queryData.count, queryData[idx] != 0 {
            let len = Int(queryData[idx]); idx += 1 + len
            if idx > queryData.count { return Data() }
        }
        let qnameEnd = idx
        let qfixed = 5
        guard queryData.count >= qnameEnd + qfixed else { return Data() }

        var resp = Data(count: 12)
        resp[0] = queryData[0]; resp[1] = queryData[1]
        let rd = queryData[2] & 0x01
        let cd = queryData[3] & 0x10
        resp[2] = 0x80 | rd
        resp[3] = 0x83 | cd  // NXDOMAIN (RCODE = 3)
        resp[4] = 0x00; resp[5] = 0x01  // QDCOUNT = 1
        resp[6] = 0x00; resp[7] = 0x00  // ANCOUNT = 0
        resp[8] = 0x00; resp[9] = 0x00  // NSCOUNT = 0
        resp[10] = 0x00; resp[11] = 0x00 // ARCOUNT = 0
        resp.append(queryData[12..<(qnameEnd + qfixed)])
        
        return resp
    }

    private func createNoDataSOAResponse(for queryData: Data, negativeTTL: UInt32) -> Data {
        guard queryData.count >= 12 else { return Data() }
        var idx = 12
        while idx < queryData.count, queryData[idx] != 0 {
            let len = Int(queryData[idx]); idx += 1 + len
            if idx > queryData.count { return Data() }
        }
        let qnameEnd = idx
        let qfixed = 5
        guard queryData.count >= qnameEnd + qfixed else { return Data() }

        @inline(__always) func appendU16BE(_ v: UInt16, to d: inout Data) {
            d.append(UInt8(v >> 8)); d.append(UInt8(v & 0xFF))
        }
        @inline(__always) func appendU32BE(_ v: UInt32, to d: inout Data) {
            d.append(UInt8((v >> 24) & 0xFF)); d.append(UInt8((v >> 16) & 0xFF))
            d.append(UInt8((v >> 8) & 0xFF));  d.append(UInt8(v & 0xFF))
        }
        func dnsName(_ s: String) -> [UInt8] {
            var out: [UInt8] = []
            for label in s.split(separator: ".") where !label.isEmpty {
                out.append(UInt8(label.utf8.count))
                out.append(contentsOf: label.utf8)
            }
            out.append(0)
            return out
        }

        var resp = Data(count: 12)
        resp[0] = queryData[0]; resp[1] = queryData[1]
        let rd = queryData[2] & 0x01
        let cd = queryData[3] & 0x10
        resp[2] = 0x80 | rd | 0x04
        resp[3] = 0x80 | cd
        resp[4] = 0x00; resp[5] = 0x01
        resp[6] = 0x00; resp[7] = 0x00
        resp[8] = 0x00; resp[9] = 0x01
        resp[10] = 0x00; resp[11] = 0x00
        resp.append(queryData[12..<(qnameEnd + qfixed)])
        let nsName = dnsName("ns.invalid.")
        let rname  = dnsName("hostmaster.invalid.")
        resp.append(contentsOf: [0xC0, 0x0C])
        appendU16BE(6, to: &resp)
        appendU16BE(1, to: &resp)
        appendU32BE(negativeTTL, to: &resp)
        var rdata = Data()
        rdata.append(contentsOf: nsName)
        rdata.append(contentsOf: rname)
        appendU32BE(1, to: &rdata)
        appendU32BE(0, to: &rdata)
        appendU32BE(0, to: &rdata)
        appendU32BE(0, to: &rdata)
        appendU32BE(negativeTTL, to: &rdata)
        appendU16BE(UInt16(rdata.count), to: &resp)
        resp.append(rdata)
        return resp
    }

    private func createDNSResponse(for queryData: Data, ipAddress: IPv4Address) -> Data {
        guard queryData.count >= 12 else { return Data() }
        var idx = 12
        while idx < queryData.count, queryData[idx] != 0 {
            let len = Int(queryData[idx]); idx += 1 + len
            if idx > queryData.count { return Data() }
        }
        guard idx < queryData.count else { return Data() }
        let qnameEnd = idx
        let qfixed = 5
        let questionEnd = qnameEnd + qfixed
        guard queryData.count >= questionEnd else { return Data() }
        let questionSection = queryData[12..<questionEnd]
        var resp = Data(count: 12)
        resp[0] = queryData[0]; resp[1] = queryData[1]
        let rd = queryData[2] & 0x01
        let cd = queryData[3] & 0x10
        resp[2] = 0x80 | rd
        resp[3] = 0x80 | cd
        resp[4] = 0x00; resp[5] = 0x01
        resp[6] = 0x00; resp[7] = 0x01
        resp[8] = 0x00; resp[9] = 0x00
        resp[10] = 0x00; resp[11] = 0x00
        resp.append(questionSection)
        @inline(__always) func appendU16BE(_ v: UInt16, to d: inout Data) {
            d.append(UInt8(v >> 8)); d.append(UInt8(v & 0xFF))
        }
        @inline(__always) func appendU32BE(_ v: UInt32, to d: inout Data) {
            d.append(UInt8((v >> 24) & 0xFF)); d.append(UInt8((v >> 16) & 0xFF))
            d.append(UInt8((v >> 8) & 0xFF));  d.append(UInt8(v & 0xFF))
        }
        let ttlSeconds: UInt32 = 10
        resp.append(contentsOf: [0xC0, 0x0C])
        appendU16BE(1, to: &resp)
        appendU16BE(1, to: &resp)
        appendU32BE(ttlSeconds, to: &resp)
        appendU16BE(4, to: &resp)
        resp.append(ipAddress.rawValue)
        if let opt = parseQueryOPT(queryData, questionEnd: questionEnd) {
            resp[10] = 0x00; resp[11] = 0x01
            appendOPT(to: &resp, udpSize: opt.udpSize, version: opt.version, doBit: opt.doBit)
        }
        return resp
    }

    private func extractQType(from dnsQuery: Data) -> UInt16? {
        guard dnsQuery.count >= 12 else { return nil }
        var idx = 12
        while idx < dnsQuery.count {
            let len = Int(dnsQuery[idx])
            if len == 0 { break }
            idx += 1 + len
            if idx > dnsQuery.count { return nil }
        }
        guard idx + 2 < dnsQuery.count else { return nil }
        let hi = dnsQuery[idx + 1]
        let lo = dnsQuery[idx + 2]
        return (UInt16(hi) << 8) | UInt16(lo)
    }

    private func parseQueryOPT(_ queryData: Data, questionEnd: Int) -> (udpSize: UInt16, version: UInt8, doBit: Bool)? {
        guard queryData.count >= 12 else { return nil }
        let arcount = (UInt16(queryData[10]) << 8) | UInt16(queryData[11])
        guard arcount > 0 else { return nil }
        let idx = questionEnd
        guard idx + 11 <= queryData.count, queryData[idx] == 0x00 else { return nil }
        let type = (UInt16(queryData[idx+1]) << 8) | UInt16(queryData[idx+2])
        guard type == 41 else { return nil }
        let udpSize = (UInt16(queryData[idx+3]) << 8) | UInt16(queryData[idx+4])
        let version = queryData[idx+6]
        let zHi = queryData[idx+7], zLo = queryData[idx+8]
        let z = (UInt16(zHi) << 8) | UInt16(zLo)
        let doBit = (z & 0x8000) != 0
        let rdlen = (UInt16(queryData[idx+9]) << 8) | UInt16(queryData[idx+10])
        guard idx + 11 + Int(rdlen) <= queryData.count else { return nil }
        return (udpSize: udpSize == 0 ? 1232 : udpSize, version: version, doBit: doBit)
    }

    private func appendOPT(to resp: inout Data, udpSize: UInt16, version: UInt8, doBit: Bool) {
        @inline(__always) func appendU16BE(_ v: UInt16) {
            resp.append(UInt8(v >> 8)); resp.append(UInt8(v & 0xFF))
        }
        @inline(__always) func appendU32BE(_ v: UInt32) {
            resp.append(UInt8((v >> 24) & 0xFF)); resp.append(UInt8((v >> 16) & 0xFF))
            resp.append(UInt8((v >> 8) & 0xFF));  resp.append(UInt8(v & 0xFF))
        }
        resp.append(0x00)
        appendU16BE(41)
        appendU16BE(udpSize)
        var ttl: UInt32 = 0
        ttl |= UInt32(version) << 16
        if doBit { ttl |= 0x0000_8000 }
        appendU32BE(ttl)
        appendU16BE(0)
    }
}

// MARK: - 线程安全状态管理
final class StateBox: @unchecked Sendable {
    private let lock = NSLock()
    private var completed = false
    
    func tryComplete() -> Bool {
        lock.lock()
        defer { lock.unlock() }
        if completed {
            return false
        }
        completed = true
        return true
    }
}

// MARK: - DNS 工具
fileprivate func extractDomainName(from dnsQueryPayload: Data) -> String? {
    guard dnsQueryPayload.count > 12 else { return nil }
    var labels: [String] = []
    var i = 12
    while i < dnsQueryPayload.count {
        let l = Int(dnsQueryPayload[i])
        if l == 0 { break }
        i += 1
        if i + l > dnsQueryPayload.count { return nil }
        if let s = String(bytes: dnsQueryPayload[i..<(i+l)], encoding: .utf8) { labels.append(s) }
        i += l
    }
    return labels.isEmpty ? nil : labels.joined(separator: ".")
}
