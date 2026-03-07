import 'dart:convert';
import 'dart:io';
import 'package:http/http.dart' as http;
import 'package:supabase/supabase.dart';
import 'package:cron/cron.dart';
import 'package:dotenv/dotenv.dart';
import 'package:intl/intl.dart';
import 'package:googleapis_auth/auth_io.dart' as auth;

// ─── GLOBAL DEĞİŞKENLEeR ────────────────────────────────────────
late final SupabaseClient _sb;
late final String _apiFKey;
final _http = http.Client();
const _apiBase = 'https://v3.football.api-sports.io';

// ─── FCM ────────────────────────────────────────────────────────
String? _fcmAccessToken;
DateTime? _fcmTokenExpiry;
String _fcmProjectId = '';
Map<String, dynamic>? _serviceAccountJson;

// ─── REQUEST SAYACI ─────────────────────────────────────────────
int _dailyRequestCount = 0;
String _dailyRequestDate = '';
const int _dailyRequestLimit = 7200;
// 7500 - 300 güvenlik payı

// ─── ADAPTIVE POLLING STATE ─────────────────────────────────────
int _consecutiveEmptyPolls = 0;
bool _hasActiveMatches = false;

// ─── MATCH STATE TRACKING ───────────────────────────────────────
final Map<int, _MatchState> _matchStates = {};

class _MatchState {
  final int homeScore;
  final int awayScore;
  final String statusShort;
  final int elapsed;
  bool lineupsFetched;
  bool h2hFetched;
  bool standingsFetched;
  DateTime? lastEventFetch;
  // Teams cache for fallback scenarios
  Map<String, dynamic>? teams;

  _MatchState({
    required this.homeScore,
    required this.awayScore,
    required this.statusShort,
    required this.elapsed,
    this.lineupsFetched = false,
    this.h2hFetched = false,
    this.standingsFetched = false,
    this.lastEventFetch,
    this.teams,
  });
}

// ═════════════════════════════════════════════════════════════════
// 🔀 MACKOLIK ENTEGRASYONU — BYPASS FLAG
// ═════════════════════════════════════════════════════════════════
// true  = Mackolik'ten al (request tasarrufu)
// false = API-Football'dan al (eski davranış)
const bool _useMackolik = true;

// Mackolik günlük maç listesi (runtime eşleştirme için cache)
List<_MackolikMatch> _mackolikDailyCache = [];
DateTime? _mackolikCacheTime;

// Mackolik ID eşleştirme cache: apiFixtureId → mackolikMatchId
final Map<int, int> _mackolikIdCache = {};

// ─── MACKOLIK MODELLER ──────────────────────────────────────────
class _MackolikMatch {
  final int mackolikId;
  final String homeTeam;
  final String awayTeam;
  final int homeTeamMacId;
  final int awayTeamMacId;
  final int statusCode;
  final int leagueId;
  final String leagueName;

  _MackolikMatch({
    required this.mackolikId,
    required this.homeTeam,
    required this.awayTeam,
    required this.homeTeamMacId,
    required this.awayTeamMacId,
    required this.statusCode,
    required this.leagueId,
    required this.leagueName,
  });
}

// ─── MACKOLIK STATS NAME MAP ────────────────────────────────────
const _statsNameMap = {
  'Topla Oynama': 'Ball Possession',
  'Toplam Şut': 'Total Shots',
  'İsabetli Şut': 'Shots on Goal',
  'İsabetsiz Şut': 'Shots off Goal',
  'Bloke Edilen Şut': 'Blocked Shots',
  'Başarılı Paslar': 'Passes accurate',
  'Pas Başarı(%)': 'Passes %',
  'Pas Başarı %': 'Passes %',
  'Korner': 'Corner Kicks',
  'Köşe Vuruşu': 'Corner Kicks',
  'Orta': 'Crosses',
  'Faul': 'Fouls',
  'Ofsayt': 'Offsides',
  'Sarı Kart': 'Yellow Cards',
  'Kırmızı Kart': 'Red Cards',
  'Kurtarış': 'Goalkeeper Saves',
  'Tehlikeli Ataklar': 'Dangerous Attacks',
  'Ataklar': 'Attacks',
};

// ─── MACKOLIK EVENT TYPE MAP ────────────────────────────────────
const _eventTypeMap = {
  1: {'type': 'Goal', 'detail': 'Normal Goal'},
  12: {'type': 'Goal', 'detail': 'Penalty'},
  13: {'type': 'Goal', 'detail': 'Own Goal'},
  2: {'type': 'Card', 'detail': 'Yellow Card'},
  3: {'type': 'Card', 'detail': 'Red Card'},
  6: {'type': 'Card', 'detail': 'Yellow Red Card'},
  4: {'type': 'subst', 'detail': 'Substitution'},
  5: {'type': 'Var', 'detail': 'VAR Decision'},
};

final _macHeaders = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
  'Accept': 'text/html,application/json,*/*',
  'Accept-Language': 'tr-TR,tr;q=0.9,en-US;q=0.8',
};

// ─── TAKİP EDİLEN LİGLER ───────────────────────────────────────
final Set<int> _leagueIds = {
  2, 3, 848, 531, 4,
  203, 204, 205, 552, 553, 554, 206,
  39, 40, 41, 42, 43, 45, 48,
  140, 141, 143,
  141, 135, 136, 137,
  142, 78, 79, 81,
  143, 61, 62, 66,
  144, 94, 95, 96,
  145, 88, 89, 90,
  146, 144, 145,
  147, 179, 180,
  148, 218, 207, 197, 333, 106, 345, 119, 113, 103, 244, 283, 286, 210, 235, 357, 384, 318, 419, 327, 398,
  149, 128, 129, 71, 72, 73, 239, 265, 253, 268, 281, 242, 299, 13, 11,
  150, 253,
  262, 263, 16,
  98, 292, 169, 307, 310, 305, 323, 17,
  233, 288, 200, 202, 332, 12,
  188, 189,
  1, 15,
  // Meksika ligleri eklendi (Liga MX ve Liga de Expansión MX)
  262,  // Liga MX
  115,  // Liga de Expansión MX
};

final Set<int> _priorityLeagueIds = {
  203, 552, 39, 140, 135, 78, 61, 2, 3, 4,
};

final Map<int, DateTime> _standingsLastFetch = {};

// ═════════════════════════════════════════════════════════════════
// MAIN
// ═════════════════════════════════════════════════════════════════
Future<void> main() async {
  try {
    DotEnv(includePlatformEnvironment: true).load();
  } catch (_) {
    print('ℹ️ .env bulunamadı, platform environment kullanılıyor');
  }

  _apiFKey = Platform.environment['API_FOOTBALL_KEY'] ?? '';
  if (_apiFKey.isEmpty) {
    print('❌ API_FOOTBALL_KEY bulunamadı!');
    exit(1);
  }

  final sbUrl = Platform.environment['SUPABASE_URL'] ?? '';
  final sbKey = Platform.environment['SUPABASE_KEY'] ?? '';
  if (sbUrl.isEmpty || sbKey.isEmpty) {
    print('❌ SUPABASE_URL veya SUPABASE_KEY bulunamadı!');
    exit(1);
  }
  _sb = SupabaseClient(sbUrl, sbKey);

  _fcmProjectId = Platform.environment['FCM_PROJECT_ID'] ?? '';
  final saJson = Platform.environment['SERVICE_ACCOUNT_JSON'] ?? '';
  if (_fcmProjectId.isNotEmpty && saJson.isNotEmpty) {
    try {
      _serviceAccountJson = jsonDecode(saJson) as Map<String, dynamic>;
      print('🔔 FCM: Aktif (project: $_fcmProjectId)');
    } catch (e) {
      print('⚠️ FCM: SERVICE_ACCOUNT_JSON parse hatası: $e');
    }
  } else {
    print('ℹ️ FCM: Devre dışı');
  }

  print('═══════════════════════════════════════');
  print('  🚀 GoalPulse Worker v7 — Mackolik');
  print('  📊 Günlük limit: $_dailyRequestLimit');
  print('  ⚽ ${_leagueIds.length} lig takip ediliyor');
  print('  🔀 Veri kaynağı: ${_useMackolik ? "MACKOLIK (tasarruf modu)" : "API-FOOTBALL (klasik)"}');
  print('═══════════════════════════════════════');

  // Health check server
  final portEnv = Platform.environment['PORT'] ?? '8080';
  final port = int.tryParse(portEnv) ?? 8080;
  final server = await HttpServer.bind('0.0.0.0', port);
  print('🌐 Health check: http://0.0.0.0:$port');
  server.listen((req) {
    req.response
      ..statusCode = 200
      ..headers.contentType = ContentType.json
      ..write(jsonEncode({
        'status': 'ok',
        'daily_requests': _dailyRequestCount,
        'daily_limit': _dailyRequestLimit,
        'active_matches': _hasActiveMatches,
        'poll_interval': _calculatePollInterval().inSeconds,
        'match_states': _matchStates.length,
        'data_source': _useMackolik ? 'mackolik' : 'api-football',
        'mackolik_cache': _mackolikDailyCache.length,
      }))
      ..close();
  });

  // Cron — sabah sync
  final cron = Cron();
  cron.schedule(Schedule.parse('0 3 * * *'), () async {
    print('\n⏰ [06:00 TR] Sabah senkronizasyonu başlıyor...');
    _resetDailyCounter();
    await _cleanUpOldData();
    await _syncTodayMatches();
    await _syncFutureMatches();
    // Mackolik cache'ini yenile
    if (_useMackolik) await _refreshMackolikCache();
    print('✅ Sabah senkronizasyonu tamamlandı\n');
  });

  // İlk çalışma
  _resetDailyCounter();
  await _syncTodayMatches();
  await _syncFutureMatches();
  if (_useMackolik) await _refreshMackolikCache();

  // Adaptive polling
  _startAdaptivePolling();
}

// ═════════════════════════════════════════════════════════════════
// ADAPTIVE POLLING
// ═════════════════════════════════════════════════════════════════

Duration _calculatePollInterval() {
  final now = DateTime.now().toUtc().add(const Duration(hours: 3));
  final hour = now.hour;
  if (_hasActiveMatches) return const Duration(seconds: 45);
  if (hour >= 2 && hour < 9) return const Duration(minutes: 30);
  if (_consecutiveEmptyPolls > 5) return const Duration(minutes: 10);
  return const Duration(minutes: 5);
}

void _startAdaptivePolling() {
  Future.doWhile(() async {
    try {
      final interval = _calculatePollInterval();
      await Future.delayed(interval);

      if (_dailyRequestCount >= _dailyRequestLimit) {
        print('🚫 Günlük istek limiti aşıldı! ($_dailyRequestCount/$_dailyRequestLimit)');
        await Future.delayed(const Duration(minutes: 30));
        return true;
      }

      await _syncLiveMatches();
    } catch (e) {
      print('❌ Polling hatası: $e');
      await Future.delayed(const Duration(minutes: 2));
    }
    return true;
  });
}

// ═════════════════════════════════════════════════════════════════
// REQUEST COUNTER
// ═════════════════════════════════════════════════════════════════

void _resetDailyCounter() {
  // Her yerde bu pattern'ı kullan:
  final trNow = DateTime.now().toUtc().add(const Duration(hours: 3));
  final dateStr = DateFormat('yyyy-MM-dd').format(trNow);

  if (_dailyRequestDate != dateStr) {
    _dailyRequestCount = 0;
    _dailyRequestDate = dateStr;
    print('📊 İstek sayacı sıfırlandı: $dateStr');
  }
}

Future<http.Response> _apiGet(String endpoint, Map<String, String> params) async {
  final trNow = DateTime.now().toUtc().add(const Duration(hours: 3));
  final today = DateFormat('yyyy-MM-dd').format(trNow);

  if (_dailyRequestDate != today) _resetDailyCounter();
  _dailyRequestCount++;

  final uri = Uri.parse('$_apiBase/$endpoint').replace(queryParameters: params);
  final res = await _http.get(uri, headers: {
    'x-apisports-key': _apiFKey,
  }).timeout(const Duration(seconds: 15));

  final emoji = res.statusCode == 200 ? '✅' : '❌';
  print('$emoji [$_dailyRequestCount/$_dailyRequestLimit] $endpoint ${params.entries.map((e) => '${e.key}=${e.value}').join('&')}');

  return res;
}

Future<List<dynamic>> _apiGetParsed(String endpoint, Map<String, String> params) async {
  final res = await _apiGet(endpoint, params);
  if (res.statusCode != 200) return [];
  final body = jsonDecode(res.body) as Map<String, dynamic>;
  return (body['response'] as List?) ?? [];
}

// ═════════════════════════════════════════════════════════════════
// MACKOLIK — EŞLEŞTİRME + VERİ ÇEKİCİLER
// ═════════════════════════════════════════════════════════════════

/// Mackolik günlük maç listesini cache'e yükle
Future<void> _refreshMackolikCache() async {
  final now = DateTime.now();
  final trHour = now.toUtc().add(const Duration(hours: 3)).hour;

  // Bugünün maçlarını çek
  final macDateToday = '${now.day.toString().padLeft(2, '0')}/'
      '${now.month.toString().padLeft(2, '0')}/'
      '${now.year}';
  _mackolikDailyCache = [];

  // Gece 00-06 arası → dünün maçlarını da cache'e al
  if (trHour < 6) {
    final yesterday = now.subtract(const Duration(days: 1));
    final macDateYesterday = '${yesterday.day.toString().padLeft(2, '0')}/'
        '${yesterday.month.toString().padLeft(2, '0')}/'
        '${yesterday.year}';
    print('📡 Mackolik cache: Dün ($macDateYesterday) + Bugün ($macDateToday)');
    await _fetchMackolikForDate(macDateYesterday);
  } else {
    print('📡 Mackolik cache: Bugün ($macDateToday)');
  }

  await _fetchMackolikForDate(macDateToday);
  _mackolikCacheTime = DateTime.now();
  print('   ✅ Mackolik cache: ${_mackolikDailyCache.length} futbol maçı');
}

/// Tek bir tarih için Mackolik maç listesini çek ve cache'e ekle
Future<void> _fetchMackolikForDate(String macDate) async {
  final url = 'https://vd.mackolik.com/livedata?date=${Uri.encodeComponent(macDate)}';
  try {
    final res = await _http.get(Uri.parse(url), headers: {
      ..._macHeaders,
      'Referer': 'https://arsiv.mackolik.com/',
    }).timeout(const Duration(seconds: 15));
    if (res.statusCode != 200) {
      print('   ⚠️ Mackolik HTTP ${res.statusCode} ($macDate)');
      return;
    }

    final data = jsonDecode(res.body);
    final raw = data['m'] as List? ?? [];
    int added = 0;
    for (final m in raw) {
      if (m is! List || m.length < 37) continue;
      final li = m[36] is List ? m[36] as List : [];
      final sportType = int.tryParse('${li.length > 11 ? li[11] : 1}') ?? 1;
      if (sportType != 1) continue;
      final matchId = int.tryParse('${m[0]}') ?? 0;
      if (matchId == 0) continue;
      // Aynı ID zaten cache'deyse ekleme (dün+bugün çakışması)
      if (_mackolikDailyCache.any((c) => c.mackolikId == matchId)) continue;
      _mackolikDailyCache.add(_MackolikMatch(
        mackolikId: matchId,
        homeTeam: '${m[2] ?? ''}'.trim(),
        awayTeam: '${m[4] ?? ''}'.trim(),
        homeTeamMacId: int.tryParse('${m[1]}') ?? 0,
        awayTeamMacId: int.tryParse('${m[3]}') ?? 0,
        statusCode: int.tryParse('${m[5]}') ?? 0,
        leagueId: int.tryParse('${li.length > 2 ? li[2] : 0}') ?? 0,
        leagueName: '${li.length > 3 ? li[3] : ''}',
      ));
      added++;
    }
    print('   📦 $macDate: $added maç eklendi');
  } catch (e) {
    print('   ⚠️ Mackolik fetch hatası ($macDate): $e');
  }
}

/// Türkçe karakter normalizasyonu
String _normalize(String name) {
  return name.toLowerCase()
      .replaceAll('ı', 'i').replaceAll('ğ', 'g').replaceAll('ü', 'u')
      .replaceAll('ş', 's').replaceAll('ö', 'o').replaceAll('ç', 'c')
      .replaceAll('é', 'e').replaceAll('á', 'a').replaceAll('ñ', 'n')
      .replaceAll(RegExp(r'[^\w\s]'), '').replaceAll(RegExp(r'\s+'), ' ').trim();
}

/// İki takım adı benzerlik skoru (0.0 - 1.0)
double _teamSimilarity(String name1, String name2) {
  final n1 = _normalize(name1);
  final n2 = _normalize(name2);
  if (n1 == n2) return 1.0;
  if (n1.contains(n2) || n2.contains(n1)) return 0.9;
  final w1 = n1.split(' ').toSet();
  final w2 = n2.split(' ').toSet();
  final inter = w1.intersection(w2);
  final union = w1.union(w2);
  final jaccard = union.isEmpty ? 0.0 : inter.length / union.length;
  if (jaccard >= 0.5) return 0.7 + jaccard * 0.2;
  if (n1.length >= 3 && n2.length >= 3 && n1.substring(0, 3) == n2.substring(0, 3)) return 0.6;
  return jaccard * 0.5;
}

/// API-Football fixture için Mackolik ID'yi bul (cache + fuzzy match)
/// Düşürülen threshold: 0.65 → 0.55 (daha esnek eşleştirme)
Future<int?> _getMackolikId(int fixtureId, String homeTeam, String awayTeam) async {
  // Cache'de varsa direkt dön
  if (_mackolikIdCache.containsKey(fixtureId)) return _mackolikIdCache[fixtureId];
  // Mackolik cache eski veya boşsa yenile
  if (_mackolikDailyCache.isEmpty ||
      _mackolikCacheTime == null ||
      DateTime.now().difference(_mackolikCacheTime!).inMinutes > 30) {
    await _refreshMackolikCache();
  }

  // Fuzzy match - threshold düşürüldü
  _MackolikMatch? best;
  double bestScore = 0;
  for (final mac in _mackolikDailyCache) {
    final homeSim = _teamSimilarity(homeTeam, mac.homeTeam);
    final awaySim = _teamSimilarity(awayTeam, mac.awayTeam);
    final combined = (homeSim + awaySim) / 2;

    if (combined > bestScore && homeSim >= 0.5 && awaySim >= 0.5) {
      bestScore = combined;
      best = mac;
    }
  }

  // Threshold düşürüldü: 0.65 → 0.55
  if (best != null && bestScore >= 0.55) {
    _mackolikIdCache[fixtureId] = best.mackolikId;
    print('   🔗 Mackolik eşleşme: $fixtureId → ${best.mackolikId} (${(bestScore * 100).toStringAsFixed(0)}%)');
    return best.mackolikId;
  }

  print('   ⚠️ Mackolik eşleşme bulunamadı: $homeTeam vs $awayTeam (bestScore: $bestScore)');
  return null;
}

// ─── MACKOLIK VERİ ÇEKİCİLER ───────────────────────────────────

Future<Map<String, dynamic>?> _macFetchDetails(int mackolikId) async {
  final url = 'https://arsiv.mackolik.com/Match/MatchData.aspx?t=dtl&id=$mackolikId&s=0';
  try {
    final res = await _http.get(Uri.parse(url), headers: {
      ..._macHeaders,
      'Referer': 'https://arsiv.mackolik.com/Mac/$mackolikId/',
    }).timeout(const Duration(seconds: 10));
    if (res.statusCode != 200 || res.body.trim().isEmpty) return null;
    if (res.body.trim().startsWith('<')) return null;
    return jsonDecode(res.body) as Map<String, dynamic>;
  } catch (e) {
    print('   ⚠️ Mackolik details hatası ($mackolikId): $e');
    return null;
  }
}

Future<String> _macFetchStats(int mackolikId) async {
  final url = 'https://arsiv.mackolik.com/AjaxHandlers/MatchHandler.aspx?command=optaStats&id=$mackolikId';
  try {
    final res = await _http.get(Uri.parse(url), headers: {
      ..._macHeaders,
      'Referer': 'https://arsiv.mackolik.com/Mac/$mackolikId/',
    }).timeout(const Duration(seconds: 10));
    return res.statusCode == 200 ? res.body : '';
  } catch (e) {
    print('   ⚠️ Mackolik stats hatası ($mackolikId): $e');
    return '';
  }
}

Future<String> _macFetchStandings(int mackolikId) async {
  final url = 'https://arsiv.mackolik.com/AjaxHandlers/StandingHandler.aspx?command=matchStanding&id=$mackolikId&sv=1';
  try {
    final res = await _http.get(Uri.parse(url), headers: {
      ..._macHeaders,
      'Referer': 'https://arsiv.mackolik.com/Mac/$mackolikId/',
    }).timeout(const Duration(seconds: 10));
    return res.statusCode == 200 ? res.body : '';
  } catch (e) {
    print('   ⚠️ Mackolik standings hatası ($mackolikId): $e');
    return '';
  }
}

// ─── MACKOLIK DÖNÜŞÜM FONKSİYONLARI ───────────────────────────

/// Mackolik details.e → match_events satırları
List<Map<String, dynamic>> _macTransformEvents(Map<String, dynamic> details, int fixtureId, Map<String, dynamic> teams) {
  final events = details['e'] as List? ?? [];
  if (events.isEmpty) return [];

  print('   📋 Mackolik events: ${events.length} event bulundu');

  int substHome = 0, substAway = 0;
  final rows = <Map<String, dynamic>>[];
  for (final ev in events) {
    if (ev is! List || ev.length < 5) continue;
    final teamCode = ev[0] as int? ?? 0;
    final minute = ev[1];
    final playerName = ev.length > 3 ? '${ev[3] ?? ''}' : '';
    final typeCode = ev[4] as int? ?? 0;
    final extra = ev.length > 5 && ev[5] is Map ? ev[5] as Map : {};
    final teamSide = teamCode == 1 ? 'home' : 'away';
    final mapped = _eventTypeMap[typeCode] ?? {'type': 'Other', 'detail': ''};
    String eventDetail = mapped['detail']!;
    if (mapped['type'] == 'subst') {
      if (teamSide == 'home') { substHome++;
        eventDetail = 'Substitution $substHome'; }
      else { substAway++; eventDetail = 'Substitution $substAway';
      }
    }

    String? assistName;
    if (extra['astName'] != null) assistName = '${extra['astName']}';
    if (mapped['type'] == 'subst') {
      final inPlayer = playerName.isNotEmpty ? playerName : null;
      final outPlayer = extra['outName'] != null ? '${extra['outName']}' : null;
      rows.add({
        'fixture_id': fixtureId,
        'event_type': mapped['type'],
        'event_detail': eventDetail,
        'player_name': inPlayer,
        'assist_name': outPlayer,
        'elapsed_time': int.tryParse('$minute') ?? 0,
        'team_id': teams[teamSide]?['id'],
        'team_name': teams[teamSide]?['name'] ?? '',
      });
    } else {
      rows.add({
        'fixture_id': fixtureId,
        'event_type': mapped['type'],
        'event_detail': eventDetail,
        'player_name': playerName.isNotEmpty ? playerName : null,
        'assist_name': assistName,
        'elapsed_time': int.tryParse('$minute') ?? 0,
        'team_id': teams[teamSide]?['id'],
        'team_name': teams[teamSide]?['name'] ?? '',
      });
    }
  }

  return rows;
}

/// Mackolik details.h/a → match_lineups.data JSON
List<Map<String, dynamic>>? _macTransformLineups(Map<String, dynamic> details, Map<String, dynamic> teams) {
  List<Map<String, dynamic>> parsePlayers(List? arr) {
    if (arr == null) return [];
    return arr.where((p) => p is List && p.isNotEmpty).map<Map<String, dynamic>>((p) {
      return {
        'player': {
          'id': int.tryParse('${p[0]}'),
          'name': '${p.length > 1 ? p[1] : ''}',
          'number': p.length > 2 ? (int.tryParse('${p[2]}') ?? 0) : 0,
          'pos': null, 'grid': null,
        }
      };
    }).toList();
  }

  final home = parsePlayers(details['h'] as List?);
  final away = parsePlayers(details['a'] as List?);
  if (home.isEmpty && away.isEmpty) return null;

  return [
    {
      'team': {'id': teams['home']?['id'], 'name': teams['home']?['name'] ?? '', 'logo': teams['home']?['logo'] ?? '', 'colors': null},
      'coach': {'id': null, 'name': null, 'photo': null},
      'startXI': home.take(11).toList(),
      'formation': null,
      'substitutes': home.skip(11).toList(),
    },
    {
      'team': {'id': teams['away']?['id'], 'name': teams['away']?['name'] ?? '', 'logo': teams['away']?['logo'] ?? '', 'colors': null},
      'coach': {'id': null, 'name': null, 'photo': null},
      'startXI': away.take(11).toList(),
      'formation': null,
      'substitutes': away.skip(11).toList(),
    }
  ];
}

/// Mackolik optaStats HTML → match_statistics.data JSON
List<Map<String, dynamic>>? _macTransformStatistics(String html, Map<String, dynamic> teams) {
  if (html.trim().length < 20) return null;
  // JSON geliyorsa stats değil
  if (html.trim().startsWith('{') || html.trim().startsWith('[')) return null;
  final homeValues = RegExp(r'team-1-statistics-text">\s*([^<]+)\s*<')
      .allMatches(html).map((m) => m.group(1)!.trim()).toList();
  final titles = RegExp(r'statistics-title-text">\s*([^<]+)\s*<')
      .allMatches(html).map((m) => m.group(1)!.trim()).toList();
  final awayValues = RegExp(r'team-2-statistics-text">\s*([^<]+)\s*<')
      .allMatches(html).map((m) => m.group(1)!.trim()).toList();

  final count = [homeValues.length, titles.length, awayValues.length].reduce((a, b) => a < b ? a : b);
  if (count == 0) return null;

  dynamic fmtVal(String raw) {
    raw = raw.trim();
    if (raw.startsWith('%')) return '${raw.substring(1)}%';
    if (raw.contains('/')) return raw;
    final n = int.tryParse(raw);
    return n ?? raw;
  }

  final homeStats = <Map<String, dynamic>>[];
  final awayStats = <Map<String, dynamic>>[];

  for (int i = 0; i < count; i++) {
    final titleEN = _statsNameMap[titles[i]] ?? titles[i];
    homeStats.add(<String, dynamic>{'type': titleEN, 'value': fmtVal(homeValues[i])});
    awayStats.add(<String, dynamic>{'type': titleEN, 'value': fmtVal(awayValues[i])});
  }

  return [
    {'team': {'id': teams['home']?['id'], 'name': teams['home']?['name'] ?? '', 'logo': teams['home']?['logo'] ?? ''}, 'statistics': homeStats},
    {'team': {'id': teams['away']?['id'], 'name': teams['away']?['name'] ?? '', 'logo': teams['away']?['logo'] ?? ''}, 'statistics': awayStats},
  ];
}

/// Mackolik standings HTML → league_standings.data JSON
List<Map<String, dynamic>>? _macTransformStandings(String html) {
  if (html.trim().length < 50) return null;
  final standings = <Map<String, dynamic>>[];
  final rowRe = RegExp(r'<tr[^>]+class="row alt[12]"[^>]*>([\s\S]*?)<\/tr>');
  for (final row in rowRe.allMatches(html)) {
    final block = row.group(0)!;
    final teamIdM = RegExp(r'data-teamid="(\d+)"').firstMatch(block);
    if (teamIdM == null) continue;
    final teamId = int.parse(teamIdM.group(1)!);
    final rankM = RegExp(r'<td[^>]*>\s*<b>(\d+)<\/b>\s*<\/td>').firstMatch(block);
    if (rankM == null) continue;
    final rank = int.parse(rankM.group(1)!);
    final nameM = RegExp(r'target="_blank"[^>]*>\s*([^<]+?)\s*<\/a>').firstMatch(block);
    final name = nameM?.group(1)?.trim() ?? '';
    final nums = RegExp(r'<td[^>]*align="right"[^>]*>(?:<b>)?(\d+)(?:<\/b>)?<\/td>')
        .allMatches(block).map((m) => int.parse(m.group(1)!)).toList();
    if (nums.length < 5) continue;
    standings.add({
      'rank': rank,
      'team': {'id': teamId, 'name': name, 'logo': 'https://im.mackolik.com/img/logo/buyuk/$teamId.gif'},
      'points': nums[4], 'goalsDiff': 0, 'group': '', 'form': '', 'status': 'same', 'description': '',
      'all': {'played': nums[0], 'win': nums[1], 'draw': nums[2], 'lose': nums[3], 'goals': {'for': 0, 'against': 0}},
      'home': {'played': 0, 'win': 0, 'draw': 0, 'lose': 0, 'goals': {'for': 0, 'against': 0}},
      'away': {'played': 0, 'win': 0, 'draw': 0, 'lose': 0, 'goals': {'for': 0, 'against': 0}},
      'update': DateTime.now().toIso8601String(),
    });
  }
  return standings.isNotEmpty ? standings : null;
}

// ═════════════════════════════════════════════════════════════════
// MACKOLIK ENTEGRASYONLU SECONDARY FETCHERS
// ═════════════════════════════════════════════════════════════════

/// Events — Mackolik veya API-Football
/// DÜZELTME: teams parametresi zorunlu değil, null gelirse Mackolik atlanıp direkt API-Football çalışır
Future<void> _fetchAndSaveEvents(int fixtureId, {Map<String, dynamic>? teams}) async {
  // Önce Mackolik dene (teams varsa)
  if (_useMackolik && teams != null) {
    final homeTeam = teams['home']?['name'] as String? ?? '';
    final awayTeam = teams['away']?['name'] as String? ?? '';
    final macId = await _getMackolikId(fixtureId, homeTeam, awayTeam);
    if (macId != null) {
      try {
        final details = await _macFetchDetails(macId);
        if (details != null) {
          final rows = _macTransformEvents(details, fixtureId, teams);
          if (rows.isNotEmpty) {
            await _sb.from('match_events').delete().eq('fixture_id', fixtureId);
            await _sb.from('match_events').insert(rows);
            print('   📋 Events [MAC]: $fixtureId → ${rows.length} event');
            return;
            // Başarılı, API-Football'a gerek yok
          } else {
            print('   ⚠️ Mackolik events boş, API-Football deneniyor...');
          }
        }
      } catch (e) {
        print('   ⚠️ Mackolik events hatası ($fixtureId): $e');
      }
      // Mackolik başarısız veya boş → API-Football fallback
      print('   🔄 Mackolik başarısız, API-Football fallback...');
    }
  } else {
    print('   ℹ️ Mackolik atlandı (teams null veya _useMackolik=false), API-Football kullanılıyor');
  }

  // ── API-FOOTBALL (fallback) ──
  try {
    final data = await _apiGetParsed('fixtures/events', {'fixture': '$fixtureId'});
    if (data.isEmpty) {
      print('   ⚠️ Events boş: $fixtureId (API-Football)');
      return;
    }

    await _sb.from('match_events').delete().eq('fixture_id', fixtureId);
    final rows = data.map((e) => {
      'fixture_id': fixtureId,
      'event_type': e['type'] ?? '',
      'event_detail': e['detail'] ?? '',
      'elapsed_time': e['time']?['elapsed'],
      'extra_time': e['time']?['extra'],
      'player_name': e['player']?['name'] ?? '',
      'player_id': e['player']?['id'],
      'assist_name': e['assist']?['name'],
      'assist_id': e['assist']?['id'],
      'team_id': e['team']?['id'],
      'team_name': e['team']?['name'] ?? '',
    }).toList();

    if (rows.isNotEmpty) {
      await _sb.from('match_events').insert(rows);
      print('   📋 Events [API]: $fixtureId → ${rows.length} event');
    }
    _detectRedCards(fixtureId, data);
  } catch (e) {
    print('⚠️ Events hatası ($fixtureId): $e');
  }
}

/// Statistics — Mackolik veya API-Football
Future<void> _fetchAndSaveStatistics(int fixtureId, {Map<String, dynamic>? teams}) async {
  // ── MACKOLIK MODU ──
  if (_useMackolik && teams != null) {
    final homeTeam = teams['home']?['name'] as String? ?? '';
    final awayTeam = teams['away']?['name'] as String? ?? '';
    final macId = await _getMackolikId(fixtureId, homeTeam, awayTeam);
    if (macId != null) {
      try {
        final html = await _macFetchStats(macId);
        final statsData = _macTransformStatistics(html, teams);
        if (statsData != null) {
          await _sb.from('match_statistics').upsert({
            'fixture_id': fixtureId,
            'data': statsData,
            'updated_at': DateTime.now().toIso8601String(),
          }, onConflict: 'fixture_id');
          print('   📊 Stats [MAC]: $fixtureId güncellendi');
          return;
        }
      } catch (e) {
        print('   ⚠️ Mackolik stats hatası ($fixtureId): $e');
      }
      print('   🔄 Mackolik stats boş, API-Football fallback...');
    }
  }

  // ── API-FOOTBALL (bypass değilse veya fallback) ──
  try {
    final data = await _apiGetParsed('fixtures/statistics', {'fixture': '$fixtureId'});
    if (data.isEmpty) return;
    await _sb.from('match_statistics').upsert({
      'fixture_id': fixtureId,
      'data': data,
      'updated_at': DateTime.now().toIso8601String(),
    }, onConflict: 'fixture_id');
    print('   📊 Stats [API]: $fixtureId güncellendi');
  } catch (e) {
    print('⚠️ Stats hatası ($fixtureId): $e');
  }
}

/// Lineups — Mackolik veya API-Football
Future<void> _fetchAndSaveLineups(int fixtureId, {Map<String, dynamic>? teams}) async {
  // ── MACKOLIK MODU ──
  if (_useMackolik && teams != null) {
    final homeTeam = teams['home']?['name'] as String? ?? '';
    final awayTeam = teams['away']?['name'] as String? ?? '';
    final macId = await _getMackolikId(fixtureId, homeTeam, awayTeam);
    if (macId != null) {
      try {
        final details = await _macFetchDetails(macId);
        if (details != null) {
          final lineupsData = _macTransformLineups(details, teams);
          if (lineupsData != null) {
            await _sb.from('match_lineups').upsert({
              'fixture_id': fixtureId,
              'data': lineupsData,
              'updated_at': DateTime.now().toIso8601String(),
            }, onConflict: 'fixture_id');
            print('   👕 Lineups [MAC]: $fixtureId kaydedildi');
            return;
          }
        }
      } catch (e) {
        print('   ⚠️ Mackolik lineups hatası ($fixtureId): $e');
      }
      print('   🔄 Mackolik lineups boş, API-Football fallback...');
    }
  }

  // ── API-FOOTBALL (bypass değilse veya fallback) ──
  try {
    final data = await _apiGetParsed('fixtures/lineups', {'fixture': '$fixtureId'});
    if (data.isEmpty) return;
    await _sb.from('match_lineups').upsert({
      'fixture_id': fixtureId,
      'data': data,
      'updated_at': DateTime.now().toIso8601String(),
    }, onConflict: 'fixture_id');
    print('   👕 Lineups [API]: $fixtureId kaydedildi');
  } catch (e) {
    print('⚠️ Lineups hatası ($fixtureId): $e');
  }
}

/// Standings — Mackolik veya API-Football
Future<void> _fetchAndSaveStandings(int fixtureId, int? leagueId, {Map<String, dynamic>? teams}) async {
  if (leagueId == null) return;
  final lastFetch = _standingsLastFetch[leagueId];
  if (lastFetch != null && DateTime.now().difference(lastFetch).inHours < 2) {
    print('   🏆 Standings: league $leagueId → 2 saat içinde zaten çekildi, SKIP');
    return;
  }

  // ── MACKOLIK MODU ──
  if (_useMackolik && teams != null) {
    final homeTeam = teams['home']?['name'] as String? ?? '';
    final awayTeam = teams['away']?['name'] as String? ?? '';
    final macId = await _getMackolikId(fixtureId, homeTeam, awayTeam);
    if (macId != null) {
      try {
        final html = await _macFetchStandings(macId);
        final data = _macTransformStandings(html);
        if (data != null) {
          await _sb.from('league_standings').upsert({
            'league_id': leagueId,
            'data': data,
            'updated_at': DateTime.now().toIso8601String(),
          }, onConflict: 'league_id');
          _standingsLastFetch[leagueId] = DateTime.now();
          print('   🏆 Standings [MAC]: league $leagueId güncellendi');
          return;
        }
      } catch (e) {
        print('   ⚠️ Mackolik standings hatası ($fixtureId): $e');
      }
      print('   🔄 Mackolik standings boş, API-Football fallback...');
    }
  }

  // ── API-FOOTBALL (bypass değilse veya fallback) ──
  try {
    final season = _getCurrentSeason();
    final data = await _apiGetParsed('standings', {'league': '$leagueId', 'season': '$season'});
    if (data.isEmpty) return;

    final standings = data[0]?['league']?['standings'];
    if (standings == null) return;
    final flatList = standings is List && standings.isNotEmpty
        ?
(standings[0] is List ? standings[0] : standings) : [];

    await _sb.from('league_standings').upsert({
      'league_id': leagueId,
      'data': flatList,
      'updated_at': DateTime.now().toIso8601String(),
    }, onConflict: 'league_id');
    _standingsLastFetch[leagueId] = DateTime.now();
    print('   🏆 Standings [API]: league $leagueId güncellendi');
  } catch (e) {
    print('⚠️ Standings hatası ($fixtureId): $e');
  }
}

/// H2H — her zaman API-Football (Flutter'da da var ama sabah sync için)
Future<void> _fetchAndSaveH2H(int fixtureId, int homeId, int awayId) async {
  // H2H Mackolik'e geçmiyor — Flutter zaten hallediyor, burası sabah sync
  final h2hKey = '$homeId-$awayId';
  try {
    final data = await _apiGetParsed('fixtures/headtohead', {'h2h': h2hKey, 'last': '10'});
    await _sb.from('match_h2h').upsert({
      'h2h_key': h2hKey,
      'data': data,
      'updated_at': DateTime.now().toIso8601String(),
    }, onConflict: 'h2h_key');
    print('   🔄 H2H: $h2hKey kaydedildi');
  } catch (e) {
    print('⚠️ H2H hatası ($h2hKey): $e');
  }
}

// ═════════════════════════════════════════════════════════════════
// SYNC LIVE MATCHES — Ana döngü
// ═════════════════════════════════════════════════════════════════

Future<void> _syncLiveMatches() async {
  final response = await _apiGetParsed('fixtures', {'live': 'all'});
  final matches = response.where((m) {
    final leagueId = m['league']?['id'] as int?;
    return leagueId != null && _leagueIds.contains(leagueId);
  }).toList();
  _hasActiveMatches = matches.isNotEmpty;
  final liveIds = matches.map((m) => m['fixture']?['id'] as int).toSet();

  if (matches.isEmpty) {
    _consecutiveEmptyPolls++;
    if (_consecutiveEmptyPolls == 1 || _consecutiveEmptyPolls % 10 == 0) {
      print('💤 Aktif maç yok (boş poll #$_consecutiveEmptyPolls)');
    }
    await _detectDisappearedMatches({});
    await _checkFinishedMatches();
    return;
  }

  _consecutiveEmptyPolls = 0;
  print('⚽ ${matches.length} aktif maç bulundu');
  for (final match in matches) {
    await _processLiveMatch(match);
  }
  await _detectDisappearedMatches(liveIds);
}

/// Tek bir canlı maçı işle
Future<void> _processLiveMatch(Map<String, dynamic> match) async {
  final fixture = match['fixture'] as Map<String, dynamic>;
  final fixtureId = fixture['id'] as int;
  final status = fixture['status'] as Map<String, dynamic>;
  final statusShort = status['short'] as String? ?? '';
  final elapsed = (status['elapsed'] as num?)?.toInt() ?? 0;
  final goals = match['goals'] as Map<String, dynamic>? ?? {};
  final homeScore = (goals['home'] as num?)?.toInt() ?? 0;
  final awayScore = (goals['away'] as num?)?.toInt() ?? 0;
  final league = match['league'] as Map<String, dynamic>? ?? {};
  final teams = match['teams'] as Map<String, dynamic>? ?? {};
  final prev = _matchStates[fixtureId];

  final scoreChanged = prev == null || prev.homeScore != homeScore || prev.awayScore != awayScore;
  final statusChanged = prev == null || prev.statusShort != statusShort;
  final isNewMatch = prev == null;

  // Teams bilgisini cache'le
  final teamsCache = teams;

  // ── 1) live_matches güncelle (her poll'da) ──
  try {
    await _sb.from('live_matches').upsert({
      'fixture_id': fixtureId,
      'home_team': teams['home']?['name'] ?? '',
      'away_team': teams['away']?['name'] ?? '',
      'home_team_id': teams['home']?['id'],
      'away_team_id': teams['away']?['id'],
      'home_logo': teams['home']?['logo'] ?? '',
      'away_logo': teams['away']?['logo'] ?? '',
      'home_score': homeScore,
      'away_score': awayScore,
      'status_short': statusShort,
      'elapsed_time': elapsed,
      'status_extra': status['extra'],
      'league_id': league['id'],
      'league_name': league['name'] ?? '',
      'league_logo': league['logo'] ?? '',
      'raw_data': jsonEncode(match),
      'updated_at': DateTime.now().toIso8601String(),
    }, onConflict: 'fixture_id');
  } catch (e) {
    print('⚠️ Upsert hatası ($fixtureId): $e');
  }

  // ── 2) SKOR DEĞİŞTİ → events + statistics ──
  if (scoreChanged && !isNewMatch) {
    print('⚽ GOL! $fixtureId: ${prev?.homeScore}-${prev?.awayScore} → $homeScore-$awayScore');
    await _fetchAndSaveEvents(fixtureId, teams: teams);
    await _fetchAndSaveStatistics(fixtureId, teams: teams);

    final homeName = teams['home']?['name'] ?? '';
    final awayName = teams['away']?['name'] ?? '';
    await _sendMatchNotification(
      fixtureId: fixtureId,
      title: '⚽ GOL!',
      body: '$homeName $homeScore - $awayScore $awayName ($elapsed\')',
      type: 'goal',
    );
  }

  // ── 3) MAÇ YENİ BAŞLADI → lineups ──
  if ((statusShort == '1H' || statusShort == 'LIVE') && (prev?.lineupsFetched != true)) {
    await _fetchAndSaveLineups(fixtureId, teams: teams);
    _matchStates[fixtureId]?.lineupsFetched = true;

    final homeName = teams['home']?['name'] ?? '';
    final awayName = teams['away']?['name'] ?? '';
    await _sendMatchNotification(
      fixtureId: fixtureId,
      title: '🟢 Maç Başladı!',
      body: '$homeName vs $awayName',
      type: 'kick_off',
    );
    // H2H — hâlâ API-Football (Flutter da hallediyor)
    if (prev?.h2hFetched != true) {
      final homeId = teams['home']?['id'] as int?;
      final awayId = teams['away']?['id'] as int?;
      if (homeId != null && awayId != null) {
        await _fetchAndSaveH2H(fixtureId, homeId, awayId);
      }
      _matchStates[fixtureId]?.h2hFetched = true;
    }
  }

  // ── 4) DEVRE ARASI → statistics ──
  if (statusShort == 'HT' && prev?.statusShort != 'HT') {
    print('⏸ Devre arası: $fixtureId');
    await _fetchAndSaveStatistics(fixtureId, teams: teams);
    await _fetchAndSaveEvents(fixtureId, teams: teams);

    final homeName = teams['home']?['name'] ?? '';
    final awayName = teams['away']?['name'] ?? '';
    await _sendMatchNotification(
      fixtureId: fixtureId,
      title: '⏸ İlk Yarı Sonucu',
      body: '$homeName $homeScore - $awayScore $awayName',
      type: 'half_time',
    );
  }

  // ── 5) MAÇ BİTTİ → son veriler + standings ──
  if (_isFinished(statusShort) && prev != null && !_isFinished(prev.statusShort)) {
    print('🏁 Maç bitti: $fixtureId ($statusShort)');
    await _fetchAndSaveEvents(fixtureId, teams: teams);
    await _fetchAndSaveStatistics(fixtureId, teams: teams);
    await _fetchAndSaveStandings(fixtureId, league['id'] as int?, teams: teams);
    _matchStates[fixtureId]?.standingsFetched = true;
    final homeName = teams['home']?['name'] ?? '';
    final awayName = teams['away']?['name'] ?? '';
    final suffix = statusShort == 'AET' ? ' (UZ)' : statusShort == 'PEN' ? ' (PEN)' : '';
    await _sendMatchNotification(
      fixtureId: fixtureId,
      title: '🏁 Maç Sonucu$suffix',
      body: '$homeName $homeScore - $awayScore $awayName',
      type: 'full_time',
    );
  }

  // ── 5b) UZATMA ──
  if (statusShort == 'ET' && prev?.statusShort != 'ET') {
    final homeName = teams['home']?['name'] ?? '';
    final awayName = teams['away']?['name'] ?? '';
    await _sendMatchNotification(
      fixtureId: fixtureId,
      title: '⏱ Uzatmalar!',
      body: '$homeName $homeScore - $awayScore $awayName — Uzatma başladı',
      type: 'extra_time',
    );
  }

  // ── 5c) PERİYODİK EVENT KONTROL ──
  final lastEvFetch = prev?.lastEventFetch;
  final shouldFetchEvents = !scoreChanged && !statusChanged && prev != null && elapsed > 0 &&
      (lastEvFetch == null || DateTime.now().difference(lastEvFetch).inMinutes >= 15);
  if (shouldFetchEvents) {
    await _fetchAndSaveEvents(fixtureId, teams: teams);
  }

  // ── 6) State güncelle ──
  _matchStates[fixtureId] = _MatchState(
    homeScore: homeScore,
    awayScore: awayScore,
    statusShort: statusShort,
    elapsed: elapsed,
    lineupsFetched: prev?.lineupsFetched ?? (statusShort != 'NS'),
    h2hFetched: prev?.h2hFetched ?? false,
    standingsFetched: prev?.standingsFetched ?? false,
    lastEventFetch: (scoreChanged || statusChanged || shouldFetchEvents) ? DateTime.now() : prev?.lastEventFetch,
    teams: teamsCache,
  );
}

// ═════════════════════════════════════════════════════════════════
// BİTEN MAÇLARI TESPİT ET
// ═════════════════════════════════════════════════════════════════

Future<void> _checkFinishedMatches() async {
  final pendingIds = _matchStates.entries
      .where((e) => _isFinished(e.value.statusShort) && !e.value.standingsFetched)
      .map((e) => e.key).toList();
  if (pendingIds.isEmpty) return;
  print('🏁 ${pendingIds.length} bitmiş maçın final işlemleri yapılıyor...');
  for (final id in pendingIds.take(5)) {
    try {
      final row = await _sb.from('live_matches').select('league_id, home_team, home_team_id, away_team, away_team_id, home_logo, away_logo').eq('fixture_id', id).maybeSingle();
      if (row != null) {
        print('🏁 Final: $id — events + stats + standings');

        // Teams bilgisini cache'ten veya DB'den al
        Map<String, dynamic>? teams = _matchStates[id]?.teams;
        if (teams == null || teams.isEmpty) {
          // DB'den al
          teams = {
            'home': {
              'id': row['home_team_id'],
              'name': row['home_team'],
              'logo': row['home_logo'],
            },
            'away': {
              'id': row['away_team_id'],
              'name': row['away_team'],
              'logo': row['away_logo'],
            },
          };
        }

        await _fetchAndSaveEvents(id, teams: teams);
        await _fetchAndSaveStatistics(id, teams: teams);
        await _fetchAndSaveStandings(id, row['league_id'] as int?, teams: teams);
        _matchStates[id]?.standingsFetched = true;
      }
    } catch (e) {
      print('⚠️ Bitiş kontrolü hatası ($id): $e');
    }
  }
}

bool _isFinished(String status) {
  return ['FT', 'AET', 'PEN', 'PST', 'CANC', 'ABD', 'AWD', 'WO'].contains(status);
}

// ═════════════════════════════════════════════════════════════════
// KIRMIZI KART TESPİTİ
// ═════════════════════════════════════════════════════════════════

final Map<int, Set<String>> _sentRedCardKeys = {};
void _detectRedCards(int fixtureId, List<dynamic> events) {
  _sentRedCardKeys[fixtureId] ??= {};
  final sent = _sentRedCardKeys[fixtureId]!;
  for (final e in events) {
    final type = e['type'] ?? '';
    final detail = e['detail'] ?? '';
    if (type == 'Card' && (detail.contains('Red') || detail.contains('Yellow-Red'))) {
      final playerName = e['player']?['name'] ?? 'Bilinmeyen';
      final teamName = e['team']?['name'] ?? '';
      final minute = e['time']?['elapsed'] ?? 0;
      final key = '$fixtureId-$playerName-$minute';
      if (sent.contains(key)) continue;
      sent.add(key);

      final state = _matchStates[fixtureId];
      final score = '${state?.homeScore ?? '?'}-${state?.awayScore ?? '?'}';
      _sendMatchNotification(
        fixtureId: fixtureId,
        title: '🟥 Kırmızı Kart!',
        body: '$playerName ($teamName) $minute\' — Skor: $score',
        type: 'red_card',
      );
    }
  }
}

// ═════════════════════════════════════════════════════════════════
// SABAH SENKRONİZASYONU
// ═════════════════════════════════════════════════════════════════

Future<void> _syncTodayMatches() async {
  final now = DateTime.now();
  // TR saatine göre bugün (sunucu UTC'de olsa bile)
  final trNow = now.toUtc().add(const Duration(hours: 3));
  final todayStr = DateFormat('yyyy-MM-dd').format(trNow);
  final trHour = now.toUtc().add(const Duration(hours: 3)).hour;

  // Gece 00:00-06:00 TR arası → dünün geç maçları hâlâ devam edebilir
  // Hem bugünü hem dünü çek
  final datesToFetch = <String>[todayStr];
  if (trHour < 6) {
    final yesterdayStr = DateFormat('yyyy-MM-dd').format(now.subtract(const Duration(days: 1)));
    datesToFetch.insert(0, yesterdayStr);
    print('🌙 Gece modu: Dünün devam eden maçları da kontrol ediliyor ($yesterdayStr)');
  }

  int totalMatches = 0;
  for (final dateStr in datesToFetch) {
    print('📅 Maçlar çekiliyor: $dateStr');
    final data = await _apiGetParsed('fixtures', {'date': dateStr, 'timezone': 'Europe/Istanbul'});
    final matches = data.where((m) {
      final lid = m['league']?['id'] as int?;
      return lid != null && _leagueIds.contains(lid);
    }).toList();
    // Dünün maçlarından sadece henüz bitmemiş olanları al
    final isYesterday = dateStr != todayStr;
    final filteredMatches = isYesterday
        ?
            matches.where((m) {
            final st = m['fixture']?['status']?['short'] as String? ?? 'FT';
            return !_isFinished(st); // Sadece hâlâ devam edenler
          }).toList()
        : matches;
    // Bugünün tamamı

    print('📋 $dateStr: ${filteredMatches.length} maç ${isYesterday ? "(devam eden)" : "(toplam)"}');
    for (final match in filteredMatches) {
      final fixture = match['fixture'] as Map<String, dynamic>;
      final fixtureId = fixture['id'] as int;
      final status = fixture['status'] as Map<String, dynamic>;
      final statusShort = status['short'] as String? ?? 'NS';
      final goals = match['goals'] as Map<String, dynamic>? ?? {};
      final teams = match['teams'] as Map<String, dynamic>? ?? {};
      final league = match['league'] as Map<String, dynamic>? ?? {};

      try {
        await _sb.from('live_matches').upsert({
          'fixture_id': fixtureId,
          'home_team': teams['home']?['name'] ?? '',
          'away_team': teams['away']?['name'] ?? '',
          'home_team_id': teams['home']?['id'],
          'away_team_id': teams['away']?['id'],
          'home_logo': teams['home']?['logo'] ?? '',
          'away_logo': teams['away']?['logo'] ?? '',
          'home_score': (goals['home'] as num?)?.toInt() ?? 0,
          'away_score': (goals['away'] as num?)?.toInt() ?? 0,
          'status_short': statusShort,
          'elapsed_time': (status['elapsed'] as num?)?.toInt(),
          'league_id': league['id'],
          'league_name': league['name'] ?? '',
          'league_logo': league['logo'] ?? '',
          'raw_data': jsonEncode(match),
          'updated_at': DateTime.now().toIso8601String(),
        }, onConflict: 'fixture_id');
      } catch (e) {
        print('⚠️ Today upsert hatası ($fixtureId): $e');
      }

      // H2H sadece öncelikli ligler
      final matchLeagueId = league['id'] as int?;
      final homeId = teams['home']?['id'] as int?;
      final awayId = teams['away']?['id'] as int?;
      if (homeId != null && awayId != null && matchLeagueId != null && _priorityLeagueIds.contains(matchLeagueId)) {
        await _fetchAndSaveH2H(fixtureId, homeId, awayId);
      }

      _matchStates[fixtureId] = _MatchState(
        homeScore: (goals['home'] as num?)?.toInt() ?? 0,
        awayScore: (goals['away'] as num?)?.toInt() ?? 0,
        statusShort: statusShort,
        elapsed: (status['elapsed'] as num?)?.toInt() ?? 0,
        h2hFetched: matchLeagueId != null && _priorityLeagueIds.contains(matchLeagueId),
        teams: teams,
      );
      totalMatches++;
    }
  }

  print('✅ Toplam $totalMatches maç live_matches\'a yazıldı');
}

Future<void> _syncFutureMatches() async {
  print('🔮 Gelecek maçlar çekiliyor...');
  final now = DateTime.now();

  for (int i = 1; i <= 4; i++) {
    final date = now.add(Duration(days: i));
    final dateStr = DateFormat('yyyy-MM-dd').format(date);

    final data = await _apiGetParsed('fixtures', {'date': dateStr, 'timezone': 'Europe/Istanbul'});
    final filtered = data.where((m) {
      final lid = m['league']?['id'] as int?;
      return lid != null && _leagueIds.contains(lid);
    }).toList();
    for (final match in filtered) {
      final fId = match['fixture']?['id'];
      if (fId == null) continue;
      try {
        await _sb.from('future_matches').upsert({
          'fixture_id': fId,
          'date': dateStr,
          'league_id': match['league']?['id'],
          'data': match,
          'updated_at': DateTime.now().toIso8601String(),
        }, onConflict: 'fixture_id');
      } catch (e) {
        print('⚠️ Future upsert hatası ($fId): $e');
      }
    }
    print('   📅 $dateStr: ${filtered.length} maç');
  }
  print('✅ Gelecek maçlar kaydedildi');
}

// ═════════════════════════════════════════════════════════════════
// HELPERS
// ═════════════════════════════════════════════════════════════════

int _getCurrentSeason() {
  final now = DateTime.now();
  return now.month >= 7 ? now.year : now.year - 1;
}

Future<void> _cleanUpOldData() async {
  // TR saatine göre bugün (sunucu UTC'de olsa bile)
  final trNow = DateTime.now().toUtc().add(const Duration(hours: 3));
  final todayStr = DateFormat('yyyy-MM-dd').format(trNow);
  // Dün gece yarısı değil, 2 gün öncesi — gece geç başlayan maçlar korunsun
  final twoDaysAgo = DateFormat('yyyy-MM-dd').format(trNow.subtract(const Duration(days: 2)));
  final cutoff = '${twoDaysAgo}T00:00:00';

  print('🧹 Eski veriler temizleniyor (2 gün öncesi)...');

  // live_matches: CANLI MAÇLARI ASLA SİLME — sadece bitmiş + 2 gün öncesini sil
  try {
    await _sb.from('live_matches').delete()
        .lt('updated_at', cutoff)
        .inFilter('status_short', ['FT', 'AET', 'PEN', 'PST', 'CANC', 'ABD', 'AWD', 'WO']);
    print('  🗑️ live_matches temizlendi (sadece bitmiş + eski)');
  } catch (e) {
    print('  ⚠️ live_matches temizleme hatası: $e');
  }

  // Diğer tablolar: 2 gün öncesini sil
  for (final table in ['match_statistics', 'match_lineups', 'match_h2h']) {
    try {
      await _sb.from(table).delete().lt('updated_at', cutoff);
      print('  🗑️ $table temizlendi');
    } catch (e) {
      print('  ⚠️ $table temizleme hatası: $e');
    }
  }

  try {
    await _sb.from('match_events').delete().lt('created_at', cutoff);
    print('  🗑️ match_events temizlendi');
  } catch (e) {
    print('  ⚠️ match_events temizleme hatası: $e');
  }

  try {
    await _sb.from('future_matches').delete().lt('date', todayStr);
    print('  🗑️ future_matches temizlendi');
  } catch (e) {
    print('  ⚠️ future_matches temizleme hatası: $e');
  }
}

// ═════════════════════════════════════════════════════════════════
// KAYIP MAÇ TESPİTİ
// ═════════════════════════════════════════════════════════════════

Future<void> _detectDisappearedMatches(Set<int> currentLiveIds) async {
  final disappeared = _matchStates.entries.where((e) {
    final status = e.value.statusShort;
    final isLiveStatus = ['1H', '2H', 'HT', 'ET', 'BT', 'P', 'LIVE'].contains(status);
    return isLiveStatus && !currentLiveIds.contains(e.key);
  }).map((e) => e.key).toList();
  if (disappeared.isEmpty) return;
  print('🔍 ${disappeared.length} maç live=all\'dan düştü');

  for (final fixtureId in disappeared.take(3)) {
    try {
      final data = await _apiGetParsed('fixtures', {'id': '$fixtureId'});
      if (data.isEmpty) continue;

      final match = data.first as Map<String, dynamic>;
      final status = match['fixture']?['status'] as Map<String, dynamic>? ?? {};
      final statusShort = status['short'] as String? ?? 'FT';
      final elapsed = (status['elapsed'] as num?)?.toInt() ?? 90;
      final goals = match['goals'] as Map<String, dynamic>? ?? {};
      final homeScore = (goals['home'] as num?)?.toInt() ?? 0;
      final awayScore = (goals['away'] as num?)?.toInt() ?? 0;
      final teams = match['teams'] as Map<String, dynamic>? ?? {};
      final league = match['league'] as Map<String, dynamic>? ?? {};
      final prev = _matchStates[fixtureId];
      await _sb.from('live_matches').upsert({
        'fixture_id': fixtureId,
        'home_score': homeScore,
        'away_score': awayScore,
        'status_short': statusShort,
        'elapsed_time': elapsed,
        'raw_data': jsonEncode(match),
        'updated_at': DateTime.now().toIso8601String(),
      }, onConflict: 'fixture_id');
      if (_isFinished(statusShort) && prev != null && !_isFinished(prev.statusShort)) {
        print('🏁 Maç bitti: $fixtureId ($statusShort)');
        final homeName = teams['home']?['name'] ?? '';
        final awayName = teams['away']?['name'] ?? '';
        final suffix = statusShort == 'AET' ? ' (UZ)' : statusShort == 'PEN' ? ' (PEN)' : '';
        await _sendMatchNotification(
          fixtureId: fixtureId,
          title: '🏁 Maç Sonucu$suffix',
          body: '$homeName $homeScore - $awayScore $awayName',
          type: 'full_time',
        );
        await _fetchAndSaveEvents(fixtureId, teams: teams);
        await _fetchAndSaveStatistics(fixtureId, teams: teams);
        await _fetchAndSaveStandings(fixtureId, league['id'] as int?, teams: teams);
        _matchStates[fixtureId]?.standingsFetched = true;
      }

      _matchStates[fixtureId] = _MatchState(
        homeScore: homeScore, awayScore: awayScore, statusShort: statusShort, elapsed: elapsed,
        lineupsFetched: true, h2hFetched: true, standingsFetched: _isFinished(statusShort),
        teams: teams,
      );
    } catch (e) {
      print('⚠️ Kayıp maç kontrol hatası ($fixtureId): $e');
    }
  }

  for (final fixtureId in disappeared.skip(3)) {
    final prev = _matchStates[fixtureId];
    if (prev == null) continue;
    print('   ⏭ $fixtureId → API çağırmadan FT yazılıyor');
    try {
      await _sb.from('live_matches').upsert({
        'fixture_id': fixtureId, 'status_short': 'FT', 'elapsed_time': 90,
        'updated_at': DateTime.now().toIso8601String(),
      }, onConflict: 'fixture_id');
    } catch (_) {}
    _matchStates[fixtureId] = _MatchState(
      homeScore: prev.homeScore, awayScore: prev.awayScore, statusShort: 'FT', elapsed: 90,
      lineupsFetched: true, h2hFetched: true, standingsFetched: true,
    );
  }
}

// ═════════════════════════════════════════════════════════════════
// FCM BİLDİRİMLER
// ═════════════════════════════════════════════════════════════════

Future<String?> _getFcmAccessToken() async {
  if (_serviceAccountJson == null) return null;
  if (_fcmAccessToken != null && _fcmTokenExpiry != null &&
      DateTime.now().isBefore(_fcmTokenExpiry!.subtract(const Duration(minutes: 5)))) {
    return _fcmAccessToken;
  }
  try {
    final credentials = auth.ServiceAccountCredentials.fromJson(_serviceAccountJson!);
    final scopes = ['https://www.googleapis.com/auth/firebase.messaging'];
    final client = await auth.clientViaServiceAccount(credentials, scopes);
    _fcmAccessToken = client.credentials.accessToken.data;
    _fcmTokenExpiry = client.credentials.accessToken.expiry;
    client.close();
    return _fcmAccessToken;
  } catch (e) {
    print('❌ FCM token hatası: $e');
    return null;
  }
}

Future<void> _sendMatchNotification({
  required int fixtureId, required String title, required String body, required String type,
}) async {
  if (_fcmProjectId.isEmpty || _serviceAccountJson == null) return;
  final token = await _getFcmAccessToken();
  if (token == null) return;

  final topic = 'match_$fixtureId';
  try {
    final uri = Uri.parse('https://fcm.googleapis.com/v1/projects/$_fcmProjectId/messages:send');
    final payload = {
      'message': {
        'topic': topic,
        'notification': {'title': title, 'body': body},
        'data': {'type': type, 'fixture_id': '$fixtureId', 'click_action': 'FLUTTER_NOTIFICATION_CLICK'},
        'android': {'priority': 'high', 'notification': {'channel_id': 'match_updates', 'sound': type == 'goal' ?
'goal_sound' : 'default'}},
        'apns': {'payload': {'aps': {'sound': type == 'goal' ?
'goal_sound.wav' : 'default', 'badge': 1}}},
      },
    };
    final res = await _http.post(uri, headers: {'Authorization': 'Bearer $token', 'Content-Type': 'application/json'}, body: jsonEncode(payload));
    if (res.statusCode == 200) print('🔔 Bildirim: [$type] $topic');
    else if (res.statusCode == 404) print('🔔 $topic → abone yok');
    else print('⚠️ FCM hatası (${res.statusCode}): ${res.body}');
  } catch (e) {
    print('⚠️ FCM gönderim hatası: $e');
  }
}
