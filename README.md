# vpn2socks

![Swift](https://img.shields.io/badge/Swift-5.7-orange.svg)
![Platform](https://img.shields.io/badge/Platform-iOS%20%7C%20macOS-blue.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

A high-performance, Swift-native VPN-to-SOCKS proxy engine for Apple platforms. `vpn2socks` was created to provide a modern, efficient, and Swift-friendly alternative to traditional `tun2socks` implementations. While projects like `tun2socks` are powerful, they are typically written in languages like C or Go, making them difficult to integrate, develop, and maintain within native Swift applications. `vpn2socks` solves this by offering a pure Swift solution designed for the NetworkExtension framework.

---

## ✅ Core Features

`vpn2socks` is built around an intelligent TCP connection manager that provides exceptional performance and resource efficiency, especially on memory-constrained mobile devices.

* **🚀 High-Performance TCP Stack**: A lightweight, custom TCP/IP stack built from the ground up in Swift to handle TCP connections efficiently within a NetworkExtension environment.

* **🧠 Intelligent & Adaptive Buffer Management**:
    * [cite_start]**100ms Fast Shrink**: Buffers are aggressively shrunk after just 100ms of inactivity to minimize the app's memory footprint[cite: 3]. [cite_start]This is confirmed in logs where connections are rapidly "shrunk to 4096" or "shrunk to 32768" after idle periods[cite: 3].
    * [cite_start]**Predictive Expansion**: Buffer sizes grow dynamically based on real-time traffic analysis and predictive algorithms[cite: 2]. [cite_start]This prevents packet loss by anticipating demand without over-allocating memory[cite: 2].
    * [cite_start]**Zero-Latency Recovery**: An emergency expansion mechanism instantly resizes buffers upon overflow, ensuring seamless performance during sudden traffic spikes[cite: 3]. [cite_start]Logs show the system detecting an `[EMERGENCY] Overflow` and immediately expanding the buffer from 32KB to 98KB and beyond[cite: 3].

* **📱 Application-Specific Optimizations**:
    * [cite_start]**YouTube Optimization**: Automatically detects YouTube video streams, assigning a dedicated 192KB buffer and handling batched requests to ensure smooth, uninterrupted playback[cite: 2]. [cite_start]Log entries frequently show `[YouTube] Confirmed batch pattern`, demonstrating this awareness[cite: 3].
    * [cite_start]**Social Media Awareness**: Identifies traffic from major social media apps to adapt buffer sizes for different user activities, such as scrolling through feeds versus watching videos[cite: 2]. This ensures a fluid user experience while conserving resources.
    * [cite_start]**Differentiated Service**: Applies different buffer strategies based on the type of service, with custom minimums for YouTube (48KB), social media (32KB), and general traffic (4KB)[cite: 2].

* **🛡️ Robust Memory & Connection Management**:
    * [cite_start]Proactively monitors device memory pressure and intelligently trims non-essential connections to maintain stability under critical conditions[cite: 2].
    * [cite_start]Includes a specific bypass for Apple Push Notification Service (APNs) traffic to ensure system notifications are delivered reliably and are not routed through the SOCKS proxy[cite: 2].
    * [cite_start]Includes a performance testing framework to analyze buffer behavior, connection lifecycle, and overall efficiency[cite: 1].

---

## Why vpn2socks?

For developers working on VPN or proxy tools for iOS and macOS, integrating a `tun2socks` component can be a significant challenge. `vpn2socks` offers a compelling alternative:

* **Swift Native**: Built entirely in modern, memory-safe Swift. This eliminates the need for complex bridging headers and makes the code easier to debug, maintain, and integrate into your existing projects.
* **Seamless Integration**: Designed specifically for Apple's `NetworkExtension` framework.
* [cite_start]**Optimized for Mobile**: The architecture is tailored for the resource constraints of mobile devices, with a strong focus on minimizing memory and battery consumption[cite: 2].
* **No External Dependencies**: Avoids the complexities of compiling and linking C or Go libraries for multiple Apple architectures.