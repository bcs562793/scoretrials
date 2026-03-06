 import 'dart:convert';
import 'dart:io';
import 'package:http/http.dart' as http;
import 'package:supabase/supabase.dart';

/// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
/// ScorePop â€” Mackolik Live Data Pipeline (Dart)
///
/// Tek script, tablo gereksiz. Her Ã§alÄ±ÅŸtÄ±ÄŸÄ±nda:
///   1. Supabase live_matches â†’ canlÄ± maÃ§larÄ± al
///   2. Mackolik livedata API â†’ gÃ¼nÃ¼n maÃ§larÄ±nÄ± al
///   3. Runtime'da eÅŸleÅŸtir (takÄ±m adÄ± + tarih fuzzy match)
///   4. Mackolik'ten events, stats, lineups, h2h, standings Ã§ek
///   5. Supabase'e yaz (mevcut tablo ÅŸemalarÄ±na birebir)
///
/// KullanÄ±m:
///   dart run mackolik_pipeline.dart
///   dart run mackolik_pipeline.dart --fixture=1491915 --mackolik=4432754
/// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

// â”€â”€â”€ CONFIG â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
final supabaseUrl = Platform.environment['SUPABASE_URL'] ?? '';
final supabaseKey = Platform.environment['SUPABASE_SERVICE_ROLE_KEY'] ?? '';

late final SupabaseClient supabase;

final macHeaders = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
  'Accept': 'text/html,application/json,*/*',
  'Accept-Language': 'tr-TR,tr;q=0.9,en-US;q=0.8',
};

// â”€â”€â”€ STATS NAME MAP (Mackolik TR â†’ API-Football EN) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
const statsNameMap = {
  'Topla Oynama': 'Ball Possession',
  'Toplam Åut': 'Total Shots',
  'Ä°sabetli Åut': 'Shots on Goal',
  'Ä°sabetsiz Åut': 'Shots off Goal',
  'Bloke Edilen Åut': 'Blocked Shots',
  'BaÅŸarÄ±lÄ± Paslar': 'Passes accurate',
  'Pas BaÅŸarÄ±(%)': 'Passes %',
  'Pas BaÅŸarÄ± %': 'Passes %',
  'Korner': 'Corner Kicks',
  'KÃ¶ÅŸe VuruÅŸu': 'Corner Kicks',
  'Orta': 'Crosses',
  'Faul': 'Fouls',
  'Ofsayt': 'Offsides',
  'SarÄ± Kart': 'Yellow Cards',
  'KÄ±rmÄ±zÄ± Kart': 'Red Cards',
  'KurtarÄ±ÅŸ': 'Goalkeeper Saves',
  'Tehlikeli Ataklar': 'Dangerous Attacks',
  'Ataklar': 'Attacks',
};

// â”€â”€â”€ EVENT TYPE MAP â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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


// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// 1. MACKOLIK GÃœNLÃœK MAÃ‡ LÄ°STESÄ° (EÅŸleÅŸtirme iÃ§in)
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

/// Mackolik'ten bugÃ¼nÃ¼n tÃ¼m maÃ§larÄ±nÄ± Ã§eker
/// DÃ¶nen format: [{mackolikId, homeTeam, awayTeam, homeMacId, awayMacId}]
Future<List<MackolikMatch>> fetchMackolikDailyMatches() async {
  final now = DateTime.now();
  final macDate = '${now.day.toString().padLeft(2, '0')}/'
      '${now.month.toString().padLeft(2, '0')}/'
      '${now.year}';

  final url = 'https://vd.mackolik.com/livedata?date=${Uri.encodeComponent(macDate)}';
  log('ğŸ“¡ Mackolik livedata Ã§ekiliyor: $macDate');

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

    log('   âœ… Mackolik: ${matches.length} futbol maÃ§Ä± bulundu');
    return matches;
  } catch (e) {
    logErr('Mackolik livedata hatasÄ±: $e');
    return [];
  }
}


// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// 2. EÅLEÅTÄ°RME (Runtime - Tablo Gereksiz)
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

/// TÃ¼rkÃ§e karakter normalizasyonu
String normalize(String name) {
  return name
      .toLowerCase()
      .replaceAll('Ä±', 'i')
      .replaceAll('ÄŸ', 'g')
      .replaceAll('Ã¼', 'u')
      .replaceAll('ÅŸ', 's')
      .replaceAll('Ã¶', 'o')
      .replaceAll('Ã§', 'c')
      .replaceAll('Ã©', 'e')
      .replaceAll('Ã¡', 'a')
      .replaceAll('Ã±', 'n')
      .replaceAll(RegExp(r'[^\w\s]'), '')
      .replaceAll(RegExp(r'\s+'), ' ')
      .trim();
}

/// Ä°ki takÄ±m adÄ± arasÄ±ndaki benzerlik skoru (0.0 - 1.0)
double teamSimilarity(String name1, String name2) {
  final n1 = normalize(name1);
  final n2 = normalize(name2);

  // Tam eÅŸleÅŸme
  if (n1 == n2) return 1.0;

  // Biri diÄŸerini iÃ§eriyor
  if (n1.contains(n2) || n2.contains(n1)) return 0.9;

  // Kelime kesiÅŸimi (Jaccard)
  final words1 = n1.split(' ').toSet();
  final words2 = n2.split(' ').toSet();
  final intersection = words1.intersection(words2);
  final union = words1.union(words2);
  final jaccard = union.isEmpty ? 0.0 : intersection.length / union.length;

  if (jaccard >= 0.5) return 0.7 + jaccard * 0.2;

  // Ä°lk 3 karakter
  if (n1.length >= 3 && n2.length >= 3 && n1.substring(0, 3) == n2.substring(0, 3)) {
    return 0.6;
  }

  return jaccard * 0.5;
}

/// Bir API-Football maÃ§Ä± iÃ§in en iyi Mackolik eÅŸleÅŸmesini bul
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


// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// 3. MACKOLIK VERÄ° Ã‡EKÄ°CÄ°LERÄ°
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

/// MatchData â†’ Events + Lineups (JSON)
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
    logErr('  âŒ Details $mackolikId: $e');
    return null;
  }
}

/// OptaStats â†’ Ä°statistikler (HTML)
Future<String> fetchMatchStats(int mackolikId) async {
  final url = 'https://arsiv.mackolik.com/AjaxHandlers/MatchHandler.aspx?command=optaStats&id=$mackolikId';
  try {
    final res = await http.get(Uri.parse(url), headers: {
      ...macHeaders,
      'Referer': 'https://arsiv.mackolik.com/Mac/$mackolikId/',
    });
    return res.statusCode == 200 ? res.body : '';
  } catch (e) {
    logErr('  âŒ Stats $mackolikId: $e');
    return '';
  }
}

/// Head2Head â†’ H2H (HTML)
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
    logErr('  âŒ H2H $mackolikId: $e');
    return null;
  }
}

/// Standings â†’ Puan Durumu (HTML)
Future<String> fetchMatchStandings(int mackolikId) async {
  final url = 'https://arsiv.mackolik.com/AjaxHandlers/StandingHandler.aspx?command=matchStanding&id=$mackolikId&sv=1';
  try {
    final res = await http.get(Uri.parse(url), headers: {
      ...macHeaders,
      'Referer': 'https://arsiv.mackolik.com/Mac/$mackolikId/',
    });
    return res.statusCode == 200 ? res.body : '';
  } catch (e) {
    logErr('  âŒ Standings $mackolikId: $e');
    return '';
  }
}


// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// 4. DÃ–NÃœÅÃœM: Mackolik â†’ Supabase Tablo ÅemasÄ±
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

/// EVENTS: details.e â†’ match_events satÄ±rlarÄ±
/// Åema: fixture_id, event_type, event_detail, player_name, assist_name, elapsed_time, team_id, team_name
List<Map<String, dynamic>> transformEvents(Map<String, dynamic> details, MatchResult match) {
  // Mackolik JSON yapÄ±sÄ±: events details['e'] veya details['d']['e'] iÃ§inde olabilir
  List? events;

  // Ä°lk olarak details['e'] dene
  final directEvents = details['e'] as List?;
  if (directEvents != null && directEvents.isNotEmpty) {
    events = directEvents;
  } else {
    // Alternatif olarak details['d']['e'] dene
    final matchData = details['d'] as Map<String, dynamic>?;
    if (matchData != null) {
      final nestedEvents = matchData['e'] as List?;
      if (nestedEvents != null && nestedEvents.isNotEmpty) {
        log('  â„¹ï¸ Events: details.d.e den alÄ±ndÄ±');
        events = nestedEvents;
      }
    }
  }

  if (events == null || events.isEmpty) {
    log('  âš ï¸ Events: events dizisi bulunamadÄ± veya boÅŸ');
    return [];
  }

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

    // Substitution numaralandÄ±rma
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

    // Substitution: giren oyuncu assist_name'e, Ã§Ä±kan player_name'e
    if (mapped['type'] == 'subst') {
      // Mackolik: ev[3] = giren, extra.outName = Ã§Ä±kan
      // Supabase formatÄ±: player_name = Ã§Ä±kan/giren, assist_name = diÄŸeri
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

/// LINEUPS: details.h/a â†’ match_lineups.data JSON
/// Åema: fixture_id, data (jsonb), updated_at
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

  // Mackolik JSON yapÄ±sÄ±: lineups details['h'] veya details['d']['h'] iÃ§inde olabilir
  List? homeList = details['h'] as List?;
  List? awayList = details['a'] as List?;

  // Fallback: details['d']['h'] ve details['d']['a'] dene
  if ((homeList == null || homeList.isEmpty) && (awayList == null || awayList.isEmpty)) {
    final matchData = details['d'] as Map<String, dynamic>?;
    if (matchData != null) {
      homeList = matchData['h'] as List?;
      awayList = matchData['a'] as List?;
    }
  }

  final homePlayers = parsePlayers(homeList, 11);
  final awayPlayers = parsePlayers(awayList, 11);

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

/// STATISTICS: optaStats HTML â†’ match_statistics.data JSON
/// Mackolik HTML'i parse edip API-Football formatÄ±na Ã§evir
List<Map<String, dynamic>>? transformStatistics(String html, MatchResult match) {
  if (html.trim().length < 20) return null;

  // JSON geliyorsa stats HTML deÄŸil, atla
  if (html.trim().startsWith('{') || html.trim().startsWith('[')) {
    log('  âš ï¸ Stats: JSON dÃ¶ndÃ¼, HTML bekleniyor â€” atlanÄ±yor');
    return null;
  }

  // Basit yaklaÅŸÄ±m: 3 listeyi ayrÄ± ayrÄ± Ã§ek, sonra zip'le
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

  log('  ğŸ” Stats parse: ${titles.length} istatistik bulundu');

  // 3 liste aynÄ± uzunlukta olmalÄ±
  final count = [homeValues.length, titles.length, awayValues.length].reduce((a, b) => a < b ? a : b);
  if (count == 0) {
    log('  âš ï¸ Stats: HTML parse baÅŸarÄ±sÄ±z, 0 istatistik');
    return null;
  }

  // DeÄŸer formatlama: API-Football formatÄ±na Ã§evir
  dynamic formatValue(String raw, String statType) {
    raw = raw.trim();
    // YÃ¼zde deÄŸerleri: "%50" â†’ "50%"
    if (raw.startsWith('%')) return '${raw.substring(1)}%';
    // Oran deÄŸerleri: "8/25" â†’ "8/25" (string olarak)
    if (raw.contains('/')) return raw;
    // SayÄ±sal deÄŸerler: "12" â†’ 12
    final n = int.tryParse(raw);
    if (n != null) return n;
    return raw;
  }

  // Home ve Away statistics listelerini oluÅŸtur
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

/// H2H: Head2Head HTML â†’ match_h2h.data JSON
/// Åema: h2h_key (text), data (jsonb), updated_at
List<Map<String, dynamic>>? transformH2H(String html) {
  final h2hMatches = <Map<String, dynamic>>[];

  final h2hRe = RegExp(r'AralarÄ±ndaki MaÃ§lar\s*<\/div>[\s\S]*?<table[^>]*class="md-table3"[^>]*>([\s\S]*?)<\/table>');
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

/// STANDINGS: StandingHandler HTML â†’ league_standings.data JSON
/// Åema: league_id (text), data (jsonb), updated_at
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


// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// 5. SUPABASE YAZMA
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

Future<void> writeEvents(String fixtureId, List<Map<String, dynamic>> events) async {
  if (events.isEmpty) return;
  try {
    // Mevcut eventleri sil
    await supabase.from('match_events').delete().eq('fixture_id', fixtureId);
    // Yeni eventleri yaz
    await supabase.from('match_events').insert(events);
    log('  âœ… Events: ${events.length} yazÄ±ldÄ±');
  } catch (e) {
    logErr('  âŒ Events yazma: $e');
  }
}

Future<void> writeStatistics(String fixtureId, List<Map<String, dynamic>> stats) async {
  try {
    await supabase.from('match_statistics').upsert({
      'fixture_id': fixtureId,
      'data': stats,
      'updated_at': DateTime.now().toIso8601String(),
    });
    log('  âœ… Statistics gÃ¼ncellendi');
  } catch (e) {
    logErr('  âŒ Stats yazma: $e');
  }
}

Future<void> writeLineups(String fixtureId, List<Map<String, dynamic>> lineups) async {
  try {
    await supabase.from('match_lineups').upsert({
      'fixture_id': fixtureId,
      'data': lineups,
      'updated_at': DateTime.now().toIso8601String(),
    });
    log('  âœ… Lineups gÃ¼ncellendi');
  } catch (e) {
    logErr('  âŒ Lineups yazma: $e');
  }
}

Future<void> writeH2H(String h2hKey, List<Map<String, dynamic>> h2hData) async {
  try {
    await supabase.from('match_h2h').upsert({
      'h2h_key': h2hKey,
      'data': h2hData,
      'updated_at': DateTime.now().toIso8601String(),
    });
    log('  âœ… H2H gÃ¼ncellendi ($h2hKey)');
  } catch (e) {
    logErr('  âŒ H2H yazma: $e');
  }
}

Future<void> writeStandings(String leagueId, List<Map<String, dynamic>> data) async {
  try {
    await supabase.from('league_standings').upsert({
      'league_id': leagueId,
      'data': data,
      'updated_at': DateTime.now().toIso8601String(),
    });
    log('  âœ… Standings gÃ¼ncellendi (league: $leagueId)');
  } catch (e) {
    logErr('  âŒ Standings yazma: $e');
  }
}


// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// 6. ANA PIPELINE
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

Future<void> processMatch(MatchResult match) async {
  log('\nâš½ ${match.homeTeamName} vs ${match.awayTeamName}');
  log('   API: ${match.apiFixtureId} â†’ Mackolik: ${match.mackolikId} (gÃ¼ven: ${(match.confidence * 100).toStringAsFixed(0)}%)');

  final macId = match.mackolikId;

  // 1. Details â†’ Events + Lineups
  log('   ğŸ“¥ Details Ã§ekiliyor...');
  final details = await fetchMatchDetails(macId);
  if (details != null) {
    log('   âœ… Details alÄ±ndÄ±, eventler iÅŸleniyor...');
    final events = transformEvents(details, match);
    log('   ğŸ“Š ${events.length} event bulundu');
    await writeEvents('${match.apiFixtureId}', events);

    final lineups = transformLineups(details, match);
    if (lineups != null) {
      log('   ğŸ‘¥ ${lineups.length} takÄ±m kadrosu bulundu');
      await writeLineups('${match.apiFixtureId}', lineups);
    } else {
      log('   âš ï¸ Kadro bulunamadÄ±');
    }
  } else {
    log('   âŒ Details alÄ±namadÄ±!');
  }

  await Future.delayed(Duration(milliseconds: 400 + (DateTime.now().millisecond % 400)));

  // 2. Statistics
  log('   ğŸ“Š Ä°statistikler Ã§ekiliyor...');
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

  log('   âœ… TamamlandÄ±');
}

Future<void> runPipeline() async {
  log('ğŸš€ ScorePop Mackolik Pipeline baÅŸlatÄ±ldÄ±');
  log('â° ${DateTime.now().toIso8601String()}\n');

  // 1. Supabase'den canlÄ± maÃ§larÄ± Ã§ek
  log('ğŸ“‹ Supabase live_matches sorgulanÄ±yor...');
  final liveResponse = await supabase
      .from('live_matches')
      .select()
      .inFilter('match_status', ['1H', '2H', 'HT', 'ET', 'PEN', 'LIVE']);

  final liveMatches = (liveResponse as List).map((m) => LiveMatch.fromMap(m)).toList();
  log('   ${liveMatches.length} canlÄ± maÃ§ bulundu');

  if (liveMatches.isEmpty) {
    log('ğŸ“­ CanlÄ± maÃ§ yok, Ã§Ä±kÄ±lÄ±yor.');
    return;
  }

  // 2. Mackolik'ten gÃ¼nÃ¼n maÃ§larÄ±nÄ± Ã§ek
  final mackolikMatches = await fetchMackolikDailyMatches();
  if (mackolikMatches.isEmpty) {
    log('âš ï¸  Mackolik maÃ§ listesi boÅŸ, Ã§Ä±kÄ±lÄ±yor.');
    return;
  }

  // 3. EÅŸleÅŸtir ve iÅŸle
  int matched = 0, failed = 0;

  for (final live in liveMatches) {
    final result = findMackolikMatch(live, mackolikMatches);
    if (result != null) {
      await processMatch(result);
      matched++;
      await Future.delayed(Duration(milliseconds: 800 + (DateTime.now().millisecond % 600)));
    } else {
      log('  âš ï¸  EÅŸleÅŸme bulunamadÄ±: ${live.homeTeamName} vs ${live.awayTeamName}');
      failed++;
    }
  }

  log('\nğŸ Pipeline tamamlandÄ±');
  log('   EÅŸleÅŸen: $matched | EÅŸleÅŸmeyen: $failed | Toplam: ${liveMatches.length}');
}

/// Manuel test: belirli fixture + mackolik id
Future<void> runManualTest(String fixtureId, String mackolikId) async {
  log('ğŸ§ª TEST MODU: fixture=$fixtureId mackolik=$mackolikId\n');

  // live_matches'tan bilgileri Ã§ek
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


// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
// MODELLER
// â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

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

// â”€â”€â”€ LOGGING â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
void log(String msg) => print(msg);
void logErr(String msg) => print('[ERR] $msg');


// â”€â”€â”€ ENTRY POINT â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
void main(List<String> arguments) async {
  if (supabaseUrl.isEmpty || supabaseKey.isEmpty) {
    print('âŒ SUPABASE_URL ve SUPABASE_SERVICE_ROLE_KEY environment variable\'larÄ± gerekli');
    exit(1);
  }

  supabase = SupabaseClient(supabaseUrl, supabaseKey);

  // ArgÃ¼manlarÄ± parse et
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
    print('ğŸ’¥ Kritik hata: $e');
    print(st);
    exit(1);
  }

  exit(0);
}
