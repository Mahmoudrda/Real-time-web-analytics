/*
* Author: Mahmoud Reda
* Email:  mahmoud.elrous.acc@gmail.com
* GitHub: https://github.com/Mahmoudrda
*
*
*
* Description:
  * This is the script that gets injected into the page using Google Tag Manager custom templates.
  * The script is responsible for tracking various events and data on the page.
  * It uses sendEventToBackend to send events to a backend endpoint instead of GTM dataLayer.
*
*
* The script is written in JavaScript and is compatible with all modern browsers.
*
*
*
* License:
  *Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *     http://www.apache.org/licenses/LICENSE-2.0
*/

(function() {
    'use strict';

    // Backend event sending function
    function sendEventToBackend(data) {
        const body = new URLSearchParams({
            ...data,
            event_timestamp: Date.now().toString()
        }).toString();

        fetch('https://backend-903553466558.us-central1.run.app', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded'
            },
            body: body
        })
        .then(response => response.json())
        .then(data => console.log('Logged:', data))
        .catch(error => console.error('Logging error:', error));
    }

    function startTracking(userConfig = {}) {
        // Merge default config with user config
        const config = {
            dataLayerName: 'dataLayer',
            scrollThresholds: [25, 50, 75, 100],
            scrollDirection: ['vertical', 'horizontal'],
            internalDomains: [],
            downloadExtensions: [
                "pdf", "xls", "xlsx", "doc", "docx", "txt", "rtf", "csv", "exe", 
                "key", "pps", "ppt", "pptx", "7z", "pkg", "rar", "gz", "zip", 
                "avi", "mov", "mp4", "mpeg", "mpg", "wmv", "midi", "mp3", 
                "wav", "wma"
            ],
            videoEvents: ['play', 'pause', 'ended', 'progress'],
            progressPercentage: [10, 25, 50, 75, 90, 100],
            youtube : true,
            youtubeEvents: ['ytplay', 'ytpause', 'ytended', 'ytprogress'],
            ytprogressPercentage: [10, 25, 50, 75, 90, 100],
            debounceTime: 100,
            ...userConfig // Override with user's config
        };

        let scrollTracker = config.scrollThresholds.reduce((acc, threshold) => {
            acc[threshold] = { vertical: false, horizontal: false };
            return acc;
        }, {});
        
        let lastPos = { vertical: window.scrollY, horizontal: window.scrollX };

        const pushEvent = (type, details) => {
            sendEventToBackend({
                event: `custom_${type}`,
                ...details
            });
        };
        
        const throttle = (fn, wait) => {
            let last = 0;
            return (...args) => {
                const now = Date.now();
                if (now - last >= wait) {
                    fn(...args);
                    last = now;
                }
            };
        };
        
        const resetScrollTracker = () => {
            scrollTracker = config.scrollThresholds.reduce((acc, threshold) => {
                acc[threshold] = { vertical: false, horizontal: false };
                return acc;
            }, {});
        };

        const handleClick = function (event) {
            const link = event.target.closest('a');
            if (!link) return;
        
            try {
                const url = new URL(link.href, window.location.href);
                const text = link.innerText || "";
        
                const internalDomains = config?.internalDomains || [];
                const downloadExtensions = config?.downloadExtensions || [];
        
                let isDownload = false;
        
                // Process download event only if downloadExtensions is not empty
                if (downloadExtensions.length > 0) {
                    const pathParts = url.pathname.split('/');
                    const fileName = pathParts.pop() || '';
                    const fileParts = fileName.split('.');
                    const fileExtension = fileParts.length > 1 ? fileParts.pop().toLowerCase() : '';
        
                    if (downloadExtensions.includes(fileExtension)) {
                        isDownload = true;
                        const eventDetails = {
                            event: 'file_download',
                            link_url: url.href,
                            link_domain: url.hostname,
                            file_name: fileName,
                            file_extension: fileExtension
                        }
                        if (text.trim()) {
                            eventDetails.link_text = text;
                        }

                        sendEventToBackend(eventDetails);
                    }
                }
                if ( !isDownload && internalDomains.length > 0) {
                    let isInternal = false;
                    for (let i = 0; i < internalDomains.length; i++) {
                        const domain = internalDomains[i];
                        const domainRegex = new RegExp(`(^|\\.)${domain.replace(/\./g, '\\.')}$`, 'i');
                        if (domainRegex.test(url.hostname)) {
                            isInternal = true;
                            break;
                        }
                    }
                    const isOutbound = !isInternal;
                    if (isOutbound) {
                        const eventDetailsforoutbound = {
                            event: 'click',
                            link_url: url.href,
                            link_domain: url.hostname,
                            is_outbound: isOutbound,
                        };
                        if (text.trim()) {
                            eventDetailsforoutbound.link_text = text;
                        }
                        sendEventToBackend(eventDetailsforoutbound);
                    }
                }
            } catch (error) {
                console.error('Error in handleClick:', error);
            }
        };
        
        let lastVerticalPageSize = document.documentElement.scrollHeight;  
        const handleScroll = throttle(() => {
            const trackScroll = (direction) => {
                const axis = direction === 'vertical' ? 'Y' : 'X';
                const currentPos = window[`scroll${axis}`];
                const viewportSize = direction === 'vertical' ? window.innerHeight : window.innerWidth;
                const pageSize = direction === 'vertical' 
                    ? document.documentElement.scrollHeight 
                    : document.documentElement.scrollWidth;
                if (direction === 'vertical' && pageSize !== lastVerticalPageSize) {
                    lastVerticalPageSize = pageSize;
                    return;
                }    

                const maxScroll = Math.max(pageSize - viewportSize, 0);
                const percentage = maxScroll > 0 ? (currentPos / maxScroll) * 100 : 0;
                
                if (currentPos > lastPos[direction]) {
                    if (config.scrollThresholds.length > 0) {
                        config.scrollThresholds.forEach(threshold => {
                            if (percentage >= threshold && !scrollTracker[threshold][direction] ) {
                                scrollTracker[threshold][direction] = true;
                                pushEvent('scroll', {
                                    percent_scrolled: threshold,
                                    direction
                                });
                            }
                        });
                    }
                }
                lastPos[direction] = currentPos;        
                
                if (direction === 'vertical') {
                    lastVerticalPageSize = pageSize;
                }
            };

            config.scrollDirection.forEach(trackScroll);
        }, config.debounceTime);

        const handleVideoEvents = function (event) {
            const video = event.target;
            if (!video.duration) return;
        
            const videoData = {
                videoProvider: 'html5',
                videoTitle: video.title || video.getAttribute('title') || video.getAttribute('data-title') || video.getAttribute('aria-label') || "no-title",
                videoUrl: video.src || video.currentSrc,
                videoDuration: Math.floor(video.duration),
                videoCurrentTime: Math.floor(video.currentTime),
                videoPercent: Math.floor((video.currentTime / video.duration) * 100)
            };
        
            switch (event.type) {
                case 'play':
                    // Track video start only once per video instance
                    if (video._trackedStart) return;
                    video._trackedStart = true;
                    sendEventToBackend({
                        event: 'video_start',
                        ...videoData
                    });
                    break;
                case 'pause':
                    sendEventToBackend({
                        event: 'video_pause',
                        ...videoData
                    });
                    break;
                case 'ended':
                    sendEventToBackend({
                        event: 'video_complete',
                        ...videoData
                    });
                    break;
                case 'timeupdate':
                    if (video.currentTime === video.duration) return;
                    video_percent = Math.floor((video.currentTime / video.duration) * 100);
            
                    // Track progress thresholds only once per video instance
                    if (!video._trackedProgress) {
                        video._trackedProgress = new Set();
                    }
                    config.progressPercentage.forEach(percent => {
                        if (
                            video_percent >= percent &&
                            !video._trackedProgress.has(percent)
                        ) {
                            video._trackedProgress.add(percent);
                            sendEventToBackend({
                                event: 'video_progress',
                                video_percent: percent,
                                ...videoData
                            });
                        }
                    });
                    break;
            }
        };

        function handelyoutube() {
            // Load the YouTube IFrame API
            var tag = document.createElement('script');
            tag.type = "text/javascript";
            tag.async = true;
            tag.src = document.location.protocol + "//www.youtube.com/iframe_api";
            var firstScriptTag = document.getElementsByTagName('script')[0];
            firstScriptTag.parentNode.insertBefore(tag, firstScriptTag);
        
            var youtubeIframes = Array.from(document.querySelectorAll('iframe'))
                .filter(iframe => iframe.src.includes('youtube'));
                
            youtubeIframes.forEach(iframe => {
                if (iframe.src.indexOf('?') > -1) {
                    iframe.src += '&enablejsapi=1';
                } else {
                    iframe.src += '?enablejsapi=1';
                }
            });
        
            var players = [];
        
            window.onYouTubeIframeAPIReady = function() {
                youtubeIframes.forEach(iframe => {
                    var player = new YT.Player(iframe, {
                        events: {
                            'onReady': onPlayerReady,
                            'onStateChange': onPlayerStateChange
                        }
                    });
            
                    player.video_started = false;
                    player.video_playing = false;
                    player.progressInterval = null;
                    player.tracked_percent = new Set();
            
                    players.push(player);
                });
            };
        
            function onPlayerReady(event) {
                console.log("youtube player ready");
            }
        
            function onPlayerStateChange(event) {
                var player = event.target;
                var videoData = player.getVideoData();
                var videoTitle = videoData.title;
                var currentTime = Math.floor(player.getCurrentTime());
                var duration = Math.floor(player.getDuration());
                var videoPercent = duration ? Math.floor((currentTime / duration) * 100) : 0;
        
                if (event.data === YT.PlayerState.PLAYING && config.youtubeEvents.includes('ytplay')) {
                    if (!player.video_started) {
                        player.video_started = true;
                        sendEventToBackend({
                            'event': "video_start",
                            'video_title': videoTitle,
                            'video_url': player.getVideoUrl(),
                            'video_current_time': currentTime,
                            'video_duration': duration,
                            'video_percent': videoPercent,
                            'video_provider': "youtube"
                        });
                    }
                    player.video_playing = true;
                    if (!player.progressInterval) {
                        player.progressInterval = setInterval(() => checkProgress(player), 500);
                    }
                } else if (event.data === YT.PlayerState.PAUSED && config.youtubeEvents.includes('ytpause')) {
                    player.video_playing = false;
                    clearInterval(player.progressInterval);
                    player.progressInterval = null;
                    sendEventToBackend({
                        'event': "video_pause",
                        'video_title': videoTitle,
                        'video_url': player.getVideoUrl(),
                        'video_current_time': currentTime,
                        'video_duration': duration,
                        'video_percent': videoPercent,
                        'video_provider': "youtube"
                    });
                } else if (event.data === YT.PlayerState.ENDED && config.youtubeEvents.includes('ytcomplete')) {
                    player.video_playing = false;
                    clearInterval(player.progressInterval);
                    player.progressInterval = null;
                    sendEventToBackend({
                        'event': "video_complete",
                        'video_title': videoTitle,
                        'video_url': player.getVideoUrl(),
                        'video_current_time': currentTime,
                        'video_duration': duration,
                        'video_percent': 100,
                        'video_provider': "youtube"
                    });
                }
            }
        
            function checkProgress(player) {
                if (!player.video_playing) return;
                var currentTime = player.getCurrentTime();
                var duration = player.getDuration();
                if (!duration) return;
                var youtube_video_percent = Math.floor((currentTime / duration) * 100);
        
                config.ytprogressPercentage.forEach(percent => {
                    if (youtube_video_percent >= percent && !player.tracked_percent.has(percent) && config.youtubeEvents.includes('ytprogress')) {
                        player.tracked_percent.add(percent);
                        sendEventToBackend({
                            'event': "video_progress",
                            'video_title': player.getVideoData().title,
                            'video_url': player.getVideoUrl(),
                            'video_current_time': Math.floor(currentTime),
                            'video_duration': Math.floor(duration),
                            'video_percent': youtube_video_percent,
                            'video_provider': "youtube"
                        });
                    }
                });
            }
        }
        
        // Handle click event
        document.addEventListener('click', handleClick);

        // Handle scroll event
        window.addEventListener('scroll', handleScroll, { passive: true });

        window.addEventListener("popstate", ()=> {
            resetScrollTracker();
            handleScroll();
        });
        window.addEventListener("hashchange", () => {
            resetScrollTracker();
            handleScroll();
        });

        if (history.pushState) {
            const originalPushState = history.pushState;
            history.pushState = function () {
                originalPushState.apply(this, arguments);
                resetScrollTracker();
                handleScroll();
            };
        } else if (history.replaceState) {
            const originalReplaceState = history.replaceState;
            history.replaceState = function () {
                originalReplaceState.apply(this, arguments);
                resetScrollTracker();
                handleScroll();
            };
        }
        
        // Handle HTML5 video events
        if (config.videoEvents.length > 0) {
            document.querySelectorAll('video').forEach(video => {
                config.videoEvents.forEach(evt => {
                    video.addEventListener(evt, handleVideoEvents);
                });
            });
        }
        
        if (config.videoEvents.length > 0) {
            const observer = new MutationObserver(mutations => {
                mutations.forEach(mutation => {
                    mutation.addedNodes.forEach(node => {
                        if (node.nodeType === Node.ELEMENT_NODE) {
                            if (node.tagName.toLowerCase() === 'video') {
                                config.videoEvents.forEach(evt => {
                                    node.addEventListener(evt, handleVideoEvents);
                                });
                            } else {
                                node.querySelectorAll('video').forEach(video => {
                                    config.videoEvents.forEach(evt => {
                                        video.addEventListener(evt, handleVideoEvents);
                                    });
                                });
                            }
                        }
                    });
                });
            });
            observer.observe(document.body, { childList: true, subtree: true });
        }
        
        // Handle YouTube video events
        if (config.youtubeEvents.length > 0) {
            handelyoutube();
        }
        
        console.log('Tracking started with config:', config);
    }

    // Expose startTracking to global scope
    window.startTracking = startTracking;

})();
