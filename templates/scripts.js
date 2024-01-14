    function updateMainArticle( url, method="GET" ) {
        let xmlhttp = new XMLHttpRequest();
        xmlhttp.onload = function f() {
            location.reload();
            document.getElementById("garminconnectoutput").innerHTML = "done";
        }
        xmlhttp.open(method, url, true);
        xmlhttp.send(null);
    }
    function removeCacheEntry( id ) {
        let url = '/sync/delete_cache_entry?id=' + id;
        let xmlhttp = new XMLHttpRequest();
        xmlhttp.open('DELETE', url, true);
        xmlhttp.onload = function see_result() {
            location.reload();
        }
        xmlhttp.send(null);
    }
    function procCacheEntry( id ) {
        let url = '/sync/proc?id=' + id;
        let xmlhttp = new XMLHttpRequest();
        xmlhttp.open('POST', url, true);
        xmlhttp.onload = function see_result() {
            location.reload();
            document.getElementById("garminconnectoutput").innerHTML = "done"
        }
        document.getElementById("garminconnectoutput").innerHTML = "processing..."
        xmlhttp.send(null);
    }
    function deleteEntry( url_, id ) {
        let url = '/sync/remove?url=' + url_;
        let xmlhttp = new XMLHttpRequest();
        xmlhttp.open('DELETE', url, true);
        xmlhttp.onload = function see_result() {
            removeCacheEntry( id );
        }
        xmlhttp.send(null);
    }
    function searchDiary() {
        let text_form = document.getElementById( 'search_text' );
        let url = encodeURI('/sync/search?text=' + text_form.value);
        updateMainArticle(url);
    }
    function syncAll() {
        updateMainArticle('/sync/sync', method="POST");
        document.getElementById("garminconnectoutput").innerHTML = "syncing..."
    }
    function syncName(name) {
        let url = '/sync/sync/' + name;
        updateMainArticle(url, method="POST");
        document.getElementById("garminconnectoutput").innerHTML = "syncing..."
    }
    function processAll() {
        updateMainArticle('/sync/proc_all', method="POST");
        document.getElementById("garminconnectoutput").innerHTML = "processing..."
    }
    function heartrateSync() {
        let ostr = '/sync/sync_garmin';
        let xmlhttp = new XMLHttpRequest();
        xmlhttp.open("POST", ostr, true);
        xmlhttp.onload = function nothing() {
            document.getElementById("garminconnectoutput").innerHTML = "done";
            document.getElementById("main_article").innerHTML = xmlhttp.responseText;
        }
        xmlhttp.send(null);
        document.getElementById("garminconnectoutput").innerHTML = "syncing";
    }
    function movieSync() {
        let ostr = '/sync/sync_movie';
        let xmlhttp = new XMLHttpRequest();
        xmlhttp.open("POST", ostr, true);
        xmlhttp.onload = function nothing() {
            document.getElementById("garminconnectoutput").innerHTML = "done";
            document.getElementById("main_article").innerHTML = xmlhttp.responseText;
        }
        xmlhttp.send(null);
        document.getElementById("garminconnectoutput").innerHTML = "syncing";
    }
    function calendarSync() {
        let ostr = '/sync/sync_calendar';
        let xmlhttp = new XMLHttpRequest();
        xmlhttp.open("POST", ostr, true);
        xmlhttp.onload = function nothing() {
            document.getElementById("garminconnectoutput").innerHTML = "done";
            document.getElementById("main_article").innerHTML = xmlhttp.responseText;
        }
        xmlhttp.send(null);
        document.getElementById("garminconnectoutput").innerHTML = "syncing";
    }
    function podcastSync() {
        let ostr = '/sync/sync_podcasts';
        let xmlhttp = new XMLHttpRequest();
        xmlhttp.open("POST", ostr, true);
        xmlhttp.onload = function nothing() {
            document.getElementById("garminconnectoutput").innerHTML = "done";
            document.getElementById("main_article").innerHTML = xmlhttp.responseText;
        }
        xmlhttp.send(null);
        document.getElementById("garminconnectoutput").innerHTML = "syncing";
    }
    function securitySync() {
        let ostr = '/sync/sync_security';
        let xmlhttp = new XMLHttpRequest();
        xmlhttp.open("POST", ostr, true);
        xmlhttp.onload = function nothing() {
            document.getElementById("garminconnectoutput").innerHTML = "done";
            document.getElementById("main_article").innerHTML = xmlhttp.responseText;
        }
        xmlhttp.send(null);
        document.getElementById("garminconnectoutput").innerHTML = "syncing";
    }
    function weatherSync() {
        let ostr = '/sync/sync_weather';
        let xmlhttp = new XMLHttpRequest();
        xmlhttp.open("POST", ostr, true);
        xmlhttp.onload = function nothing() {
            document.getElementById("garminconnectoutput").innerHTML = "done";
            document.getElementById("main_article").innerHTML = xmlhttp.responseText;
        }
        xmlhttp.send(null);
        document.getElementById("garminconnectoutput").innerHTML = "syncing";
    }
