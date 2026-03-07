import 'dart:convert';

import 'dart:io';

import 'package:http/http.dart' as http;

import 'package:supabase/supabase.dart';



/// ═══════════════════════════════════════════════════════════════════════════

/// ScorePop — Mackolik Live Data Pipeline (Dart)

///

/// Tek script, tablo gereksiz. Her çalıştığında:

///   1. Supabase live_matches → canlı maçları al

///   2. Mackolik livedata API → günün maçlarını al

///   3. Runtime'da eşleştir (takım adı + tarih fuzzy match)

///   4. Mackolik'ten events, stats, lineups, h2h, standings çek

///   5. Supabase'e yaz (mevcut tablo şemalarına birebir)

///

/// Kullanım:

///   dart run mackolik_pipeline.dart

///   dart run mackolik_pipeline.dart --fixture=1491915 --mackolik=4432754

/// ═══════════════════════════════════════════════════════════════════════════



// ─── CONFIG ──────────────────────────────────────────────────────────────────

final supabaseUrl = Platform.environment['SUPABASE_URL'] ?? '';

final supabaseKey = Platform.environment['SUPABASE_SERVICE_ROLE_KEY'] ?? '';



late final SupabaseClient supabase;



final macHeaders = {

  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',

  'Accept': 'text/html,application/json,*/*',

  'Accept-Language': 'tr-TR,tr;q=0.9,en-US;q=0.8',

};



// ─── STATS NAME MAP (Mackolik TR → API-Football EN) ─────────────────────────

const statsNameMap = {

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



// ─── EVENT TYPE MAP ──────────────────────────────────────────────────────────

const eventTypeMap = {

  1: {'type': 'Goal', 'detail': 'Normal Goal'},

  12: {'type': 'Goal', 'detail': 'Penalty'},

  13: {'type': 'Goal', 'detail': 'Own Goal'},

  2: {'type': 'Card', 'detail': 'Yellow Card'},

  3: {'type': 'Card', 'detail': 'Red Card'},

  6: {'type': 'Card', 'detail': 'Yellow Red Card'},

  4: {'type': 'subst', 'detail': 'Substitution'},

  5: {'type': 'Var', 'detail': 'VAR Decision'},

};





// ═══════════════════════════════════════════════════════════════════════════

// 1. MACKOLIK GÜNLÜK MAÇ LİSTESİ (Eşleştirme için)

// ═══════════════════════════════════════════════════════════════════════════



/// Mackolik'ten bugünün tüm maçlarını çeker

/// Dönen format: [{mackolikId, homeTeam, awayTeam, homeMacId, awayMacId}]

Future<List<MackolikMatch>> fetchMackolikDailyMatches() async {

  final now = DateTime.now();

  final macDate = '${now.day.toString().padLeft(2, '0')}/'

      '${now.month.toString().padLeft(2, '0')}/'

      '${now.year}';



  final url = 'https://vd.mackolik.com/livedata?date=${Uri.encodeComponent(macDate)}';

  log('📡 Mackolik livedata çekiliyor: $macDate');



  try {

    final res = await http.get(Uri.parse(url), headers: {

      ...macHeaders,

      'Referer': 'https://arsiv.mackolik.com/',

    });



    if (res.statusCode != 200) {

      logErr('Mackolik HTTP ${res.statusCode}');

      return [];

    }



    final data = jsonDecode(res.body);

    final raw = data['m'] as List? ?? [];



    final matches = <MackolikMatch>[];

    for (final m in raw) {

      if (m is! List || m.length < 37) continue;



      // Sadece futbol (sportType = 1)

      final li = m[36] is List ? m[36] as List : [];

      final sportType = int.tryParse('${li.length > 11 ? li[11] : 1}') ?? 1;

      if (sportType != 1) continue;



      final matchId = int.tryParse('${m[0]}') ?? 0;

      if (matchId == 0) continue;



      matches.add(MackolikMatch(

        mackolikId: matchId,

        homeTeam: '${m[2] ?? ''}'.trim(),

        awayTeam: '${m[4] ?? ''}'.trim(),

        homeTeamMacId: int.tryParse('${m[1]}') ?? 0,

        awayTeamMacId: int.tryParse('${m[3]}') ?? 0,

        statusCode: int.tryParse('${m[5]}') ?? 0,

        leagueId: int.tryParse('${li.length > 2 ? li[2] : 0}') ?? 0,

        leagueName: '${li.length > 3 ? li[3] : ''}',

      ));

    }



    log('   ✅ Mackolik: ${matches.length} futbol maçı bulundu');

    return matches;

  } catch (e) {

    logErr('Mackolik livedata hatası: $e');

    return [];

  }

}





// ═══════════════════════════════════════════════════════════════════════════

// 2. EŞLEŞTİRME (Runtime - Tablo Gereksiz)

// ═══════════════════════════════════════════════════════════════════════════



/// Türkçe karakter normalizasyonu

String normalize(String name) {

  return name

      .toLowerCase()

      .replaceAll('ı', 'i')

      .replaceAll('ğ', 'g')

      .replaceAll('ü', 'u')

      .replaceAll('ş', 's')

      .replaceAll('ö', 'o')

      .replaceAll('ç', 'c')

      .replaceAll('é', 'e')

      .replaceAll('á', 'a')

      .replaceAll('ñ', 'n')

      .replaceAll(RegExp(r'[^\w\s]'), '')

      .replaceAll(RegExp(r'\s+'), ' ')

      .trim();

}



/// İki takım adı arasındaki benzerlik skoru (0.0 - 1.0)

double teamSimilarity(String name1, String name2) {

  final n1 = normalize(name1);

  final n2 = normalize(name2);



  // Tam eşleşme

  if (n1 == n2) return 1.0;



  // Biri diğerini içeriyor

  if (n1.contains(n2) || n2.contains(n1)) return 0.9;



  // Kelime kesişimi (Jaccard)

  final words1 = n1.split(' ').toSet();

  final words2 = n2.split(' ').toSet();

  final intersection = words1.intersection(words2);

  final union = words1.union(words2);

  final jaccard = union.isEmpty ? 0.0 : intersection.length / union.length;



  if (jaccard >= 0.5) return 0.7 + jaccard * 0.2;



  // İlk 3 karakter

  if (n1.length >= 3 && n2.length >= 3 && n1.substring(0, 3) == n2.substring(0, 3)) {

    return 0.6;

  }



  return jaccard * 0.5;

}



/// Bir API-Football maçı için en iyi Mackolik eşleşmesini bul

MatchResult? findMackolikMatch(LiveMatch liveMatch, List<MackolikMatch> mackolikMatches) {

  MackolikMatch? bestMatch;

  double bestScore = 0;



  for (final mac in mackolikMatches) {

    final homeSim = teamSimilarity(liveMatch.homeTeamName, mac.homeTeam);

    final awaySim = teamSimilarity(liveMatch.awayTeamName, mac.awayTeam);

    final combined = (homeSim + awaySim) / 2;



    if (combined > bestScore && homeSim >= 0.5 && awaySim >= 0.5) {

      bestScore = combined;

      bestMatch = mac;

    }

  }



  if (bestMatch != null && bestScore >= 0.65) {

    return MatchResult(

      apiFixtureId: liveMatch.fixtureId,

      mackolikId: bestMatch.mackolikId,

      confidence: bestScore,

      homeTeamId: liveMatch.homeTeamId,

      awayTeamId: liveMatch.awayTeamId,

      homeTeamName: liveMatch.homeTeamName,

      awayTeamName: liveMatch.awayTeamName,

      homeTeamLogo: liveMatch.homeTeamLogo,

      awayTeamLogo: liveMatch.awayTeamLogo,

      leagueId: liveMatch.leagueId,

    );

  }

  return null;

}





// ═══════════════════════════════════════════════════════════════════════════

// 3. MACKOLIK VERİ ÇEKİCİLERİ

// ═══════════════════════════════════════════════════════════════════════════



/// MatchData → Events + Lineups (JSON)

Future<Map<String, dynamic>?> fetchMatchDetails(int mackolikId) async {

  final url = 'https://arsiv.mackolik.com/Match/MatchData.aspx?t=dtl&id=$mackolikId&s=0';

  try {

    final res = await http.get(Uri.parse(url), headers: {

      ...macHeaders,

      'Referer': 'https://arsiv.mackolik.com/Mac/$mackolikId/',

    });

    if (res.statusCode != 200 || res.body.trim().isEmpty) return null;

    if (res.body.trim().startsWith('<')) return null; // HTML = hata

    return jsonDecode(res.body) as Map<String, dynamic>;

  } catch (e) {

    logErr('  ❌ Details $mackolikId: $e');

    return null;

  }

}



/// OptaStats → İstatistikler (HTML)

Future<String> fetchMatchStats(int mackolikId) async {

  final url = 'https://arsiv.mackolik.com/AjaxHandlers/MatchHandler.aspx?command=optaStats&id=$mackolikId';

  try {

    final res = await http.get(Uri.parse(url), headers: {

      ...macHeaders,

      'Referer': 'https://arsiv.mackolik.com/Mac/$mackolikId/',

    });

    return res.statusCode == 200 ? res.body : '';

  } catch (e) {

    logErr('  ❌ Stats $mackolikId: $e');

    return '';

  }

}



/// Head2Head → H2H (HTML)

Future<String?> fetchMatchH2H(int mackolikId) async {

  final url = 'https://arsiv.mackolik.com/Match/Head2Head.aspx?id=$mackolikId&s=1';

  try {

    final res = await http.get(Uri.parse(url), headers: {

      ...macHeaders,

      'Referer': 'https://arsiv.mackolik.com/Mac/$mackolikId/',

    });

    if (res.statusCode != 200) return null;

    if (res.body.contains('Object moved') || res.body.contains('PageError.htm')) return null;

    return res.body;

  } catch (e) {

    logErr('  ❌ H2H $mackolikId: $e');

    return null;

  }

}



/// Standings → Puan Durumu (HTML)

Future<String> fetchMatchStandings(int mackolikId) async {

  final url = 'https://arsiv.mackolik.com/AjaxHandlers/StandingHandler.aspx?command=matchStanding&id=$mackolikId&sv=1';

  try {

    final res = await http.get(Uri.parse(url), headers: {

      ...macHeaders,

      'Referer': 'https://arsiv.mackolik.com/Mac/$mackolikId/',

    });

    return res.statusCode == 200 ? res.body : '';

  } catch (e) {

    logErr('  ❌ Standings $mackolikId: $e');

    return '';

  }

}





// ═══════════════════════════════════════════════════════════════════════════

// 4. DÖNÜŞÜM: Mackolik → Supabase Tablo Şeması

// ═══════════════════════════════════════════════════════════════════════════



/// EVENTS: details.e → match_events satırları

/// Şema: fixture_id, event_type, event_detail, player_name, assist_name, elapsed_time, team_id, team_name

List<Map<String, dynamic>> transformEvents(Map<String, dynamic> details, MatchResult match) {

  final events = details['e'] as List? ?? [];

  if (events.isEmpty) return [];



  int substHome = 0, substAway = 0;



  return events.map<Map<String, dynamic>>((ev) {

    if (ev is! List || ev.length < 5) return <String, dynamic>{};



    final teamCode = ev[0] as int? ?? 0; // 1=home, 2=away

    final minute = ev[1];

    final playerName = ev.length > 3 ? '${ev[3] ?? ''}' : '';

    final typeCode = ev[4] as int? ?? 0;

    final extra = ev.length > 5 && ev[5] is Map ? ev[5] as Map : {};



    final teamSide = teamCode == 1 ? 'home' : 'away';

    final mapped = eventTypeMap[typeCode] ?? {'type': 'Other', 'detail': ''};



    // Substitution numaralandırma

    String eventDetail = mapped['detail']!;

    if (mapped['type'] == 'subst') {

      if (teamSide == 'home') {

        substHome++;

        eventDetail = 'Substitution $substHome';

      } else {

        substAway++;

        eventDetail = 'Substitution $substAway';

      }

    }



    // Asist

    String? assistName;

    if (extra['astName'] != null) assistName = '${extra['astName']}';



    // Substitution: giren oyuncu assist_name'e, çıkan player_name'e

    if (mapped['type'] == 'subst') {

      // Mackolik: ev[3] = giren, extra.outName = çıkan

      // Supabase formatı: player_name = çıkan/giren, assist_name = diğeri

      final inPlayer = playerName.isNotEmpty ? playerName : null;

      final outPlayer = extra['outName'] != null ? '${extra['outName']}' : null;

      return {

        'fixture_id': '${match.apiFixtureId}',

        'event_type': mapped['type'],

        'event_detail': eventDetail,

        'player_name': inPlayer,

        'assist_name': outPlayer,

        'elapsed_time': '${int.tryParse('$minute') ?? 0}',

        'team_id': teamSide == 'home' ? match.homeTeamId : match.awayTeamId,

        'team_name': teamSide == 'home' ? match.homeTeamName : match.awayTeamName,

      };

    }



    return {

      'fixture_id': '${match.apiFixtureId}',

      'event_type': mapped['type'],

      'event_detail': eventDetail,

      'player_name': playerName.isNotEmpty ? playerName : null,

      'assist_name': assistName,

      'elapsed_time': '${int.tryParse('$minute') ?? 0}',

      'team_id': teamSide == 'home' ? match.homeTeamId : match.awayTeamId,

      'team_name': teamSide == 'home' ? match.homeTeamName : match.awayTeamName,

    };

  }).where((e) => e.isNotEmpty).toList();

}



/// LINEUPS: details.h/a → match_lineups.data JSON

/// Şema: fixture_id, data (jsonb), updated_at

List<Map<String, dynamic>>? transformLineups(Map<String, dynamic> details, MatchResult match) {

  List<Map<String, dynamic>> parsePlayers(List? arr, int startCount) {

    if (arr == null) return [];

    return arr.map<Map<String, dynamic>>((p) {

      if (p is! List || p.isEmpty) return <String, dynamic>{};

      return {

        'player': {

          'id': int.tryParse('${p[0]}'),

          'name': '${p.length > 1 ? p[1] : ''}',

          'number': p.length > 2 ? (int.tryParse('${p[2]}') ?? 0) : 0,

          'pos': null,

          'grid': null,

        }

      };

    }).where((p) => p.isNotEmpty).toList();

  }



  final homePlayers = parsePlayers(details['h'] as List?, 11);

  final awayPlayers = parsePlayers(details['a'] as List?, 11);



  if (homePlayers.isEmpty && awayPlayers.isEmpty) return null;



  return [

    {

      'team': {

        'id': match.homeTeamId,

        'name': match.homeTeamName,

        'logo': match.homeTeamLogo,

        'colors': null,

      },

      'coach': {'id': null, 'name': null, 'photo': null},

      'startXI': homePlayers.take(11).toList(),

      'formation': null,

      'substitutes': homePlayers.skip(11).toList(),

    },

    {

      'team': {

        'id': match.awayTeamId,

        'name': match.awayTeamName,

        'logo': match.awayTeamLogo,

        'colors': null,

      },

      'coach': {'id': null, 'name': null, 'photo': null},

      'startXI': awayPlayers.take(11).toList(),

      'formation': null,

      'substitutes': awayPlayers.skip(11).toList(),

    }

  ];

}



/// STATISTICS: optaStats HTML → match_statistics.data JSON

/// Mackolik HTML'i parse edip API-Football formatına çevir

List<Map<String, dynamic>>? transformStatistics(String html, MatchResult match) {

  if (html.trim().length < 20) return null;



  // JSON geliyorsa stats HTML değil, atla

  if (html.trim().startsWith('{') || html.trim().startsWith('[')) {

    log('  ⚠️ Stats: JSON döndü, HTML bekleniyor — atlanıyor');

    return null;

  }



  // Basit yaklaşım: 3 listeyi ayrı ayrı çek, sonra zip'le

  final homeValues = RegExp(r'team-1-statistics-text">\s*([^<]+)\s*<')

      .allMatches(html)

      .map((m) => m.group(1)!.trim())

      .toList();



  final titles = RegExp(r'statistics-title-text">\s*([^<]+)\s*<')

      .allMatches(html)

      .map((m) => m.group(1)!.trim())

      .toList();



  final awayValues = RegExp(r'team-2-statistics-text">\s*([^<]+)\s*<')

      .allMatches(html)

      .map((m) => m.group(1)!.trim())

      .toList();



  log('  🔍 Stats parse: ${titles.length} istatistik bulundu');



  // 3 liste aynı uzunlukta olmalı

  final count = [homeValues.length, titles.length, awayValues.length].reduce((a, b) => a < b ? a : b);

  if (count == 0) {

    log('  ⚠️ Stats: HTML parse başarısız, 0 istatistik');

    return null;

  }



  // Değer formatlama: API-Football formatına çevir

  dynamic formatValue(String raw, String statType) {

    raw = raw.trim();

    // Yüzde değerleri: "%50" → "50%"

    if (raw.startsWith('%')) return '${raw.substring(1)}%';

    // Oran değerleri: "8/25" → "8/25" (string olarak)

    if (raw.contains('/')) return raw;

    // Sayısal değerler: "12" → 12

    final n = int.tryParse(raw);

    if (n != null) return n;

    return raw;

  }



  // Home ve Away statistics listelerini oluştur

  final homeStats = <Map<String, dynamic>>[];

  final awayStats = <Map<String, dynamic>>[];



  for (int i = 0; i < count; i++) {

    final titleTR = titles[i];

    final titleEN = statsNameMap[titleTR] ?? titleTR;



    homeStats.add(<String, dynamic>{

      'type': titleEN,

      'value': formatValue(homeValues[i], titleEN),

    });

    awayStats.add(<String, dynamic>{

      'type': titleEN,

      'value': formatValue(awayValues[i], titleEN),

    });

  }



  return [

    {

      'team': {

        'id': match.homeTeamId,

        'name': match.homeTeamName,

        'logo': match.homeTeamLogo,

      },

      'statistics': homeStats,

    },

    {

      'team': {

        'id': match.awayTeamId,

        'name': match.awayTeamName,

        'logo': match.awayTeamLogo,

      },

      'statistics': awayStats,

    }

  ];

}



/// H2H: Head2Head HTML → match_h2h.data JSON

/// Şema: h2h_key (text), data (jsonb), updated_at

List<Map<String, dynamic>>? transformH2H(String html) {

  final h2hMatches = <Map<String, dynamic>>[];



  final h2hRe = RegExp(r'Aralarındaki Maçlar\s*<\/div>[\s\S]*?<table[^>]*class="md-table3"[^>]*>([\s\S]*?)<\/table>');

  final h2hSection = h2hRe.firstMatch(html);

  if (h2hSection == null) return null;



  final rowRe = RegExp(r'<tr class="row alt[12]">([\s\S]*?)<\/tr>');

  for (final row in rowRe.allMatches(h2hSection.group(1)!)) {

    final tdRe = RegExp(r'<td[^>]*>([\s\S]*?)<\/td>');

    final tds = tdRe.allMatches(row.group(1)!).map((m) => m.group(1)!).toList();

    if (tds.length < 8) continue;



    final dateRaw = tds[2].replaceAll(RegExp(r'<[^>]+>'), '').trim();

    final scoreM = RegExp(r'\/Mac\/(\d+)\/[^>]+><b>\s*(\d+)\s*-\s*(\d+)').firstMatch(tds[6]);

    if (scoreM == null) continue;



    final homeGoals = int.parse(scoreM.group(2)!);

    final awayGoals = int.parse(scoreM.group(3)!);

    final homeName = tds[5].replaceAll(RegExp(r'<[^>]+>'), '').replaceAll('&nbsp;', '').trim();

    final awayName = tds[7].replaceAll(RegExp(r'<[^>]+>'), '').replaceAll('&nbsp;', '').trim();



    final htM = tds.length > 8 ? RegExp(r'(\d+)\s*-\s*(\d+)').firstMatch(tds[8]) : null;



    h2hMatches.add({

      'fixture': {

        'id': int.parse(scoreM.group(1)!),

        'date': dateRaw,

        'venue': {'id': null, 'city': null, 'name': null},

        'status': {'long': 'Match Finished', 'short': 'FT', 'elapsed': 90, 'extra': null},

        'periods': {'first': null, 'second': null},

        'referee': null, 'timezone': 'Europe/Istanbul', 'timestamp': null,

      },

      'league': {'id': null, 'name': null, 'country': null, 'logo': null, 'flag': null, 'season': null, 'round': null, 'standings': false},

      'teams': {

        'home': {'id': null, 'name': homeName, 'logo': null, 'winner': homeGoals > awayGoals ? true : homeGoals < awayGoals ? false : null},

        'away': {'id': null, 'name': awayName, 'logo': null, 'winner': awayGoals > homeGoals ? true : awayGoals < homeGoals ? false : null},

      },

      'goals': {'home': homeGoals, 'away': awayGoals},

      'score': {

        'halftime': {'home': htM != null ? int.parse(htM.group(1)!) : null, 'away': htM != null ? int.parse(htM.group(2)!) : null},

        'fulltime': {'home': homeGoals, 'away': awayGoals},

        'extratime': {'home': null, 'away': null},

        'penalty': {'home': null, 'away': null},

      },

    });



    if (h2hMatches.length >= 10) break;

  }



  return h2hMatches.isNotEmpty ? h2hMatches : null;

}



/// STANDINGS: StandingHandler HTML → league_standings.data JSON

/// Şema: league_id (text), data (jsonb), updated_at

List<Map<String, dynamic>>? transformStandings(String html) {

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

        .allMatches(block)

        .map((m) => int.parse(m.group(1)!))

        .toList();

    if (nums.length < 5) continue;



    standings.add({

      'rank': rank,

      'team': {'id': teamId, 'name': name, 'logo': 'https://im.mackolik.com/img/logo/buyuk/$teamId.gif'},

      'points': nums[4],

      'goalsDiff': 0,

      'group': '', 'form': '', 'status': 'same', 'description': '',

      'all': {'played': nums[0], 'win': nums[1], 'draw': nums[2], 'lose': nums[3], 'goals': {'for': 0, 'against': 0}},

      'home': {'played': 0, 'win': 0, 'draw': 0, 'lose': 0, 'goals': {'for': 0, 'against': 0}},

      'away': {'played': 0, 'win': 0, 'draw': 0, 'lose': 0, 'goals': {'for': 0, 'against': 0}},

      'update': DateTime.now().toIso8601String(),

    });

  }



  return standings.isNotEmpty ? standings : null;

}





// ═══════════════════════════════════════════════════════════════════════════

// 5. SUPABASE YAZMA

// ═══════════════════════════════════════════════════════════════════════════



Future<void> writeEvents(String fixtureId, List<Map<String, dynamic>> events) async {

  if (events.isEmpty) {

    log('  ⚠️ Events: Yeni event yok, mevcut veri korunuyor');

    return;

  }

  try {

    await supabase.from('match_events').delete().eq('fixture_id', fixtureId);

    await supabase.from('match_events').insert(events);

    log('  ✅ Events: ${events.length} yazıldı');

  } catch (e) {

    logErr('  ❌ Events yazma: $e');

  }

}



Future<void> writeStatistics(String fixtureId, List<Map<String, dynamic>> stats) async {

  try {

    await supabase.from('match_statistics').upsert({

      'fixture_id': fixtureId,

      'data': stats,

      'updated_at': DateTime.now().toIso8601String(),

    });

    log('  ✅ Statistics güncellendi');

  } catch (e) {

    logErr('  ❌ Stats yazma: $e');

  }

}



Future<void> writeLineups(String fixtureId, List<Map<String, dynamic>> lineups) async {

  try {

    await supabase.from('match_lineups').upsert({

      'fixture_id': fixtureId,

      'data': lineups,

      'updated_at': DateTime.now().toIso8601String(),

    });

    log('  ✅ Lineups güncellendi');

  } catch (e) {

    logErr('  ❌ Lineups yazma: $e');

  }

}



Future<void> writeH2H(String h2hKey, List<Map<String, dynamic>> h2hData) async {

  try {

    await supabase.from('match_h2h').upsert({

      'h2h_key': h2hKey,

      'data': h2hData,

      'updated_at': DateTime.now().toIso8601String(),

    });

    log('  ✅ H2H güncellendi ($h2hKey)');

  } catch (e) {

    logErr('  ❌ H2H yazma: $e');

  }

}



Future<void> writeStandings(String leagueId, List<Map<String, dynamic>> data) async {

  try {

    await supabase.from('league_standings').upsert({

      'league_id': leagueId,

      'data': data,

      'updated_at': DateTime.now().toIso8601String(),

    });

    log('  ✅ Standings güncellendi (league: $leagueId)');

  } catch (e) {

    logErr('  ❌ Standings yazma: $e');

  }

}





// ═══════════════════════════════════════════════════════════════════════════

// 6. ANA PIPELINE

// ═══════════════════════════════════════════════════════════════════════════



Future<void> processMatch(MatchResult match) async {

  log('\n⚽ ${match.homeTeamName} vs ${match.awayTeamName}');

  log('   API: ${match.apiFixtureId} → Mackolik: ${match.mackolikId} (güven: ${(match.confidence * 100).toStringAsFixed(0)}%)');



  final macId = match.mackolikId;



  // 1. Details → Events + Lineups

  final details = await fetchMatchDetails(macId);

  if (details != null) {

    final events = transformEvents(details, match);

    await writeEvents('${match.apiFixtureId}', events);



    final lineups = transformLineups(details, match);

    if (lineups != null) await writeLineups('${match.apiFixtureId}', lineups);

  }



  await Future.delayed(Duration(milliseconds: 400 + (DateTime.now().millisecond % 400)));



  // 2. Statistics

  final statsHtml = await fetchMatchStats(macId);

  final statsData = transformStatistics(statsHtml, match);

  if (statsData != null) await writeStatistics('${match.apiFixtureId}', statsData);



  await Future.delayed(Duration(milliseconds: 400 + (DateTime.now().millisecond % 400)));



  // 3. H2H

  final h2hHtml = await fetchMatchH2H(macId);

  if (h2hHtml != null) {

    final h2hData = transformH2H(h2hHtml);

    if (h2hData != null) {

      final h2hKey = '${match.homeTeamId}-${match.awayTeamId}';

      await writeH2H(h2hKey, h2hData);

    }

  }



  await Future.delayed(Duration(milliseconds: 400 + (DateTime.now().millisecond % 400)));



  // 4. Standings

  final standingsHtml = await fetchMatchStandings(macId);

  final standingsData = transformStandings(standingsHtml);

  if (standingsData != null && match.leagueId != null) {

    await writeStandings('${match.leagueId}', standingsData);

  }



  log('   ✅ Tamamlandı');

}



Future<void> runPipeline() async {

  log('🚀 ScorePop Mackolik Pipeline başlatıldı');

  log('⏰ ${DateTime.now().toIso8601String()}\n');



  // 1. Supabase'den canlı maçları çek

  log('📋 Supabase live_matches sorgulanıyor...');

  final liveResponse = await supabase

      .from('live_matches')

      .select()

      .inFilter('match_status', ['1H', '2H', 'HT', 'ET', 'PEN', 'LIVE']);



  final liveMatches = (liveResponse as List).map((m) => LiveMatch.fromMap(m)).toList();

  log('   ${liveMatches.length} canlı maç bulundu');



  if (liveMatches.isEmpty) {

    log('📭 Canlı maç yok, çıkılıyor.');

    return;

  }



  // 2. Mackolik'ten günün maçlarını çek

  final mackolikMatches = await fetchMackolikDailyMatches();

  if (mackolikMatches.isEmpty) {

    log('⚠️  Mackolik maç listesi boş, çıkılıyor.');

    return;

  }



  // 3. Eşleştir ve işle

  int matched = 0, failed = 0;



  for (final live in liveMatches) {

    final result = findMackolikMatch(live, mackolikMatches);

    if (result != null) {

      await processMatch(result);

      matched++;

      await Future.delayed(Duration(milliseconds: 800 + (DateTime.now().millisecond % 600)));

    } else {

      log('  ⚠️  Eşleşme bulunamadı: ${live.homeTeamName} vs ${live.awayTeamName}');

      failed++;

    }

  }



  log('\n🏁 Pipeline tamamlandı');

  log('   Eşleşen: $matched | Eşleşmeyen: $failed | Toplam: ${liveMatches.length}');

}



/// Manuel test: belirli fixture + mackolik id

Future<void> runManualTest(String fixtureId, String mackolikId) async {

  log('🧪 TEST MODU: fixture=$fixtureId mackolik=$mackolikId\n');



  // live_matches'tan bilgileri çek

  final response = await supabase

      .from('live_matches')

      .select()

      .eq('fixture_id', fixtureId)

      .maybeSingle();



  final match = MatchResult(

    apiFixtureId: int.parse(fixtureId),

    mackolikId: int.parse(mackolikId),

    confidence: 1.0,

    homeTeamId: response?['home_team_id'],

    awayTeamId: response?['away_team_id'],

    homeTeamName: response?['home_team_name'] ?? 'Home',

    awayTeamName: response?['away_team_name'] ?? 'Away',

    homeTeamLogo: response?['home_team_logo'],

    awayTeamLogo: response?['away_team_logo'],

    leagueId: response?['league_id'],

  );



  await processMatch(match);

}





// ═══════════════════════════════════════════════════════════════════════════

// MODELLER

// ═══════════════════════════════════════════════════════════════════════════



class LiveMatch {

  final int fixtureId;

  final String homeTeamName;

  final String awayTeamName;

  final int? homeTeamId;

  final int? awayTeamId;

  final String? homeTeamLogo;

  final String? awayTeamLogo;

  final int? leagueId;

  final String matchStatus;



  LiveMatch({

    required this.fixtureId,

    required this.homeTeamName,

    required this.awayTeamName,

    this.homeTeamId,

    this.awayTeamId,

    this.homeTeamLogo,

    this.awayTeamLogo,

    this.leagueId,

    required this.matchStatus,

  });



  factory LiveMatch.fromMap(Map<String, dynamic> m) => LiveMatch(

        fixtureId: int.tryParse('${m['fixture_id']}') ?? 0,

        homeTeamName: m['home_team_name'] ?? '',

        awayTeamName: m['away_team_name'] ?? '',

        homeTeamId: m['home_team_id'],

        awayTeamId: m['away_team_id'],

        homeTeamLogo: m['home_team_logo'],

        awayTeamLogo: m['away_team_logo'],

        leagueId: m['league_id'],

        matchStatus: m['match_status'] ?? '',

      );

}



class MackolikMatch {

  final int mackolikId;

  final String homeTeam;

  final String awayTeam;

  final int homeTeamMacId;

  final int awayTeamMacId;

  final int statusCode;

  final int leagueId;

  final String leagueName;



  MackolikMatch({

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



class MatchResult {

  final int apiFixtureId;

  final int mackolikId;

  final double confidence;

  final int? homeTeamId;

  final int? awayTeamId;

  final String homeTeamName;

  final String awayTeamName;

  final String? homeTeamLogo;

  final String? awayTeamLogo;

  final int? leagueId;



  MatchResult({

    required this.apiFixtureId,

    required this.mackolikId,

    required this.confidence,

    this.homeTeamId,

    this.awayTeamId,

    required this.homeTeamName,

    required this.awayTeamName,

    this.homeTeamLogo,

    this.awayTeamLogo,

    this.leagueId,

  });

}



// ─── LOGGING ────────────────────────────────────────────────────────────────

void log(String msg) => print(msg);

void logErr(String msg) => print('[ERR] $msg');





// ─── ENTRY POINT ────────────────────────────────────────────────────────────

void main(List<String> arguments) async {

  if (supabaseUrl.isEmpty || supabaseKey.isEmpty) {

    print('❌ SUPABASE_URL ve SUPABASE_SERVICE_ROLE_KEY environment variable\'ları gerekli');

    exit(1);

  }



  supabase = SupabaseClient(supabaseUrl, supabaseKey);



  // Argümanları parse et

  final args = <String, String>{};

  for (final arg in arguments) {

    if (arg.startsWith('--')) {

      final parts = arg.substring(2).split('=');

      args[parts[0]] = parts.length > 1 ? parts.sublist(1).join('=') : 'true';

    }

  }



  try {

    if (args.containsKey('fixture') && args.containsKey('mackolik')) {

      // Manuel test modu

      await runManualTest(args['fixture']!, args['mackolik']!);

    } else {

      // Normal pipeline

      await runPipeline();

    }

  } catch (e, st) {

    print('💥 Kritik hata: $e');

    print(st);

    exit(1);

  }



  exit(0);

} 
