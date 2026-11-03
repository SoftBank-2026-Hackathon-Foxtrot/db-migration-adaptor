"""Build the implementation report with caru-ini/stack-diagram.

Usage: python scripts/build_architecture.py --skill-dir PATH --icon-dir PATH
The supplied skill directory must contain assets/template.html and scripts/build.py.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
from html import escape
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "architecture"
SOURCES = ROOT / "docs" / "architecture-source"
WIDTH = 1180


def diagram(key, title, caption, height):
    return dict(
        key=key,
        title=title,
        caption=caption,
        width=WIDTH,
        height=height,
        groups=[],
        nodes=[],
        edges=[],
        notes=[],
    )


def node(d, key, name, role, icon, x, y):
    d["nodes"].append(dict(key=key, name=name, role=role, icon=icon, x=x, y=y))


def group(d, title, x, y, w, h, external=False):
    d["groups"].append(dict(title=title, x=x, y=y, w=w, h=h, external=external))


def edge(d, path, label="", x=0, y=0, primary=False):
    d["edges"].append(dict(path=path, label=label, x=x, y=y, primary=primary))


def note(d, text, x, y, w):
    d["notes"].append(dict(text=text, x=x, y=y, w=w))


def content():
    architecture = diagram(
        "architecture",
        "1. 전체 구조",
        "API는 명령을 저장합니다. Worker가 실제 DB와 PgBouncer를 변경합니다. "
        "아래 업무 경로는 전환 전 상태입니다.",
        810,
    )
    group(architecture, "별도 구현 · 오케스트레이터", 35, 45, 190, 215, True)
    group(architecture, "DB Operator · Python 3.13", 310, 45, 780, 215)
    group(architecture, "업무 실행 주체", 35, 490, 190, 220, True)
    group(architecture, "고정 DB 접속점", 325, 490, 190, 220)
    group(architecture, "원본 · 자체 운영 PG18", 615, 490, 190, 220)
    group(architecture, "대상 · 자체 운영 PG18", 905, 490, 190, 220)
    for args in [
        ("orchestrator", "오케스트레이터", "앱 정지·확인·재개", "worker", 130, 105),
        ("api", "FastAPI", "인증·명령 접수", "fastapi", 420, 105),
        ("management", "관리 PostgreSQL", "명령·실행 의도·근거", "postgresql", 710, 105),
        ("worker", "DB Worker", "한 실행기·상태 확인", "python", 1000, 105),
        ("config", "공유 설정 디렉터리", "pgbouncer.ini", "gateway", 1000, 340),
        ("app", "앱·업무 Worker", "동일한 DB 주소 사용", "server", 130, 550),
        ("pgbouncer", "PgBouncer", "transaction pooling", "proxy", 420, 550),
        ("source", "원본 PostgreSQL", "전환 전 업무 writer", "postgresql", 710, 550),
        ("target", "대상 PostgreSQL", "준비한 빈 schema", "postgresql", 1000, 550),
    ]:
        node(architecture, *args)
    edge(architecture, "M166 135 H384", "HTTP 명령", 275, 127, True)
    edge(architecture, "M456 135 H674", "저장 / QUEUED", 565, 127)
    edge(architecture, "M964 135 H746", "작업 획득·결과", 855, 127)
    edge(architecture, "M130 211 V300 H710 V211", "SDK · HELD / RUNNING 기록", 420, 292)
    edge(architecture, "M94 135 H20 V580 H94", "업무 제어", 62, 425)
    edge(architecture, "M1000 211 V270 H825 V530 H710 V544", "관리 SQL", 865, 262)
    edge(architecture, "M1036 135 H1130 V580 H1036", "관리 SQL", 1090, 425)
    edge(architecture, "M970 211 V320 H555 V530 H420 V544", "PAUSE / RELOAD / RESUME", 735, 312)
    edge(architecture, "M1000 211 V334", "파일 교체", 1055, 302)
    edge(architecture, "M1000 446 V470 H475 V565 H456", "공유 volume", 770, 462)
    edge(architecture, "M166 580 H384", "고정 주소 · SQL", 275, 572, True)
    edge(architecture, "M456 580 H674", "원본 경로 · 전환 전", 565, 572, True)
    edge(architecture, "M746 580 H964", "초기 복사·CDC", 855, 572, True)
    edge(architecture, "M1000 656 V745 H710 V656", "선택한 역복제 · 대상 → 원본", 855, 737)
    note(
        architecture,
        "오케스트레이터와 실제 앱 정지 채널은 이 저장소가 구현하지 않습니다.",
        35,
        770,
        1090,
    )

    forward = diagram(
        "cutover",
        "2. 원본에서 대상으로 전환",
        "CUTOVER는 경로만 바꿉니다. RESUME TARGET이 DB 접속을 열고 "
        "오케스트레이터가 앱 업무를 다시 엽니다.",
        680,
    )
    group(forward, "복사 중에는 원본에서 업무를 계속 처리", 35, 45, 810, 210)
    group(forward, "업무 정지 시작", 925, 45, 220, 210, True)
    group(forward, "업무 쓰기 정지 유지 · 각 명령 완료 후 최신 상태 확인", 35, 335, 1110, 220)
    items = [
        ("prepare", "1. PREPARE", "권한·schema·범위 검사", "verify", 130, 105),
        ("sync", "2. START_SYNC", "copy_data=true", "postgresql", 430, 105),
        ("ready", "3. READY 확인", "초기 복사 상태 조회", "verify", 730, 105),
        ("app-held", "4. 앱 업무 정지", "전원 HELD · inFlight=0", "lock", 1030, 105),
        ("quiesce", "5. QUIESCE", "최종 복제·데이터 대조", "lock", 130, 395),
        ("cutover", "6. CUTOVER", "역복제 준비·경로 변경", "proxy", 430, 395),
        ("resume", "7. RESUME TARGET", "대상 writer·접속 확인", "play", 730, 395),
        ("app-running", "8. 앱 업무 재개", "오케스트레이터 실행", "server", 1030, 395),
    ]
    for args in items:
        node(forward, *args)
    for y in [135, 425]:
        for a, b in [(130, 430), (430, 730), (730, 1030)]:
            edge(forward, f"M{a + 36} {y} H{b - 36}", primary=True)
    edge(forward, "M1030 211 V290 H20 V425 H94", "quiesceRef", 575, 282, True)
    note(
        forward,
        "QUIESCE: PAUSE → writer NOLOGIN → 세션 0 → barrier 내구성 확인 → "
        "복제 정지 → sequence 동기화·데이터 digest 대조",
        60,
        580,
        1060,
    )
    note(
        forward,
        "그다음: 업무 검사 PASS + 최신 RUNNING 근거 + 30초 안정 관측 → "
        "readiness.advance=PASS → 다음 리소스",
        60,
        624,
        1060,
    )

    rollback = diagram(
        "rollback",
        "3. 롤백은 대상 쓰기 전·후로 구분",
        "원본 경로 복귀와 원본 업무 재개는 별도 명령입니다. "
        "실제 상태가 불명이면 먼저 RECONCILE이 필요합니다.",
        640,
    )
    group(rollback, "대상 쓰기 전 · 원본이 기준 DB · writer 차단 확인이 전제", 35, 40, 1110, 225)
    group(
        rollback,
        "대상 쓰기 후 · REVERSE_REQUIRED · 역복제를 대상 쓰기 전에 준비한 경우",
        35,
        330,
        1110,
        225,
    )
    for args in [
        ("held-before", "정지 상태 확인", "HELD · DB writer 차단", "lock", 130, 105),
        ("validate-before", "원본 검증", "미사용 대상 오류와 분리", "verify", 430, 105),
        ("rollback-before", "ROLLBACK", "원본 경로 · 계속 PAUSE", "proxy", 730, 105),
        ("resume-before", "RESUME SOURCE", "원본 writer·접속 확인", "play", 1030, 105),
        ("held-after", "업무 다시 정지", "generation 증가 · QUIESCE", "lock", 130, 395),
        ("validate-after", "최종 역복제 검증", "barrier·데이터·sequence", "postgresql", 430, 395),
        ("rollback-after", "ROLLBACK", "원본 경로 · 계속 PAUSE", "proxy", 730, 395),
        ("resume-after", "RESUME SOURCE", "원본 writer·접속 확인", "play", 1030, 395),
    ]:
        node(rollback, *args)
    for y in [135, 425]:
        for a, b in [(130, 430), (430, 730), (730, 1030)]:
            edge(rollback, f"M{a + 36} {y} H{b - 36}", primary=True)
    note(
        rollback,
        "역복제 또는 최신성 근거가 없으면 여기서 멈춥니다. 원본으로 자동 복귀하지 않습니다. "
        "원본 접속 확인 후 앱 업무는 별도로 재개합니다.",
        50,
        588,
        1080,
    )

    recovery = diagram(
        "recovery",
        "4. 응답을 잃으면 멈추고 실제 상태를 대조",
        "아래는 외부 변경을 시작한 뒤 응답을 확인하지 못한 경우입니다. "
        "기록만 보고 같은 변경을 자동 재실행하지 않습니다.",
        650,
    )
    group(recovery, "실행 단계 · 외부 변경 전에 관리 DB에 실행 의도 저장", 35, 45, 1110, 225)
    group(recovery, "복구 단계 · RECONCILE은 외부 자원을 변경하지 않음", 35, 335, 1110, 225)
    for args in [
        ("intent", "실행 의도 저장", "operations.external_intent", "postgresql", 130, 110),
        ("effect", "외부 명령 실행", "예: LOGIN / RESUME", "worker", 430, 110),
        ("lost", "응답 확인 실패", "적용 여부를 단정 불가", "unknown", 730, 110),
        ("unknown", "UNKNOWN", "다른 변경 명령 차단", "stop", 1030, 110),
        ("reconcile", "RECONCILE", "별도 복구 명령 접수", "verify", 1030, 400),
        ("inspect", "실제 상태 조회", "원격 세션·경로·권한", "proxy", 730, 400),
        ("compare", "기록과 상태 대조", "불명·충돌이면 계속 중단", "postgresql", 430, 400),
        ("next", "가능한 명령 확인", "allowedActions 재조회", "play", 130, 400),
    ]:
        node(recovery, *args)
    for a, b in [(130, 430), (430, 730), (730, 1030)]:
        edge(recovery, f"M{a + 36} 140 H{b - 36}", primary=True)
    edge(recovery, "M1030 216 V394", "명시적 요청", 1090, 312, True)
    for a, b in [(1030, 730), (730, 430), (430, 130)]:
        edge(recovery, f"M{a - 36} 430 H{b + 36}", primary=True)
    note(
        recovery,
        "이전 원격 실행 연결이 남았거나 경로·권한이 충돌하면 복구 완료로 판정하지 않습니다. "
        "대상 쓰기 가능 기록은 응답 유실 뒤에도 보존합니다.",
        50,
        590,
        1080,
    )
    return [architecture, forward, rollback, recovery]


def canvas(d):
    key = d["key"]
    parts = [
        f'<div class="scroller"><div class="canvas" id="{key}" '
        f'style="width:{WIDTH}px;height:{d["height"]}px" role="img" '
        f'aria-label="{escape(d["title"] + ". " + d["caption"])}">'
    ]
    for g in d["groups"]:
        kind = "external" if g["external"] else "gb"
        parts.append(
            f'<div class="group {kind}" style="left:{g["x"]}px;top:{g["y"]}px;'
            f'width:{g["w"]}px;height:{g["h"]}px">'
            f'<div class="g-label">{escape(g["title"])}</div></div>'
        )
    parts.append(
        f'<svg class="edges" viewBox="0 0 {WIDTH} {d["height"]}" aria-hidden="true">'
        f'<defs><marker id="ar-{key}" viewBox="0 0 10 10" refX="8" refY="5" '
        'markerWidth="6.5" markerHeight="6.5" orient="auto-start-reverse">'
        '<path d="M0 0 L10 5 L0 10 z" fill="context-stroke"/></marker></defs>'
    )
    for e in d["edges"]:
        cls = "e" if e["primary"] else "n"
        parts.append(f'<path class="{cls}" d="{e["path"]}" marker-end="url(#ar-{key})"/>')
        if e["label"]:
            cls = "el a" if e["primary"] else "el"
            parts.append(
                f'<text class="{cls}" x="{e["x"]}" y="{e["y"]}" '
                f'text-anchor="middle">{escape(e["label"])}</text>'
            )
    parts.append("</svg>")
    for n in d["nodes"]:
        parts.append(
            f'<div class="node" data-node="{n["key"]}" '
            f'style="left:{n["x"] - 80}px;top:{n["y"]}px">'
            f'<div class="tile"><img src="%%ICON_{n["icon"]}%%" alt=""></div>'
            f'<div class="name"><span>{escape(n["name"])}</span></div>'
            f'<div class="role"><span>{escape(n["role"])}</span></div></div>'
        )
    for n in d["notes"]:
        parts.append(
            f'<p class="canvas-note" style="left:{n["x"]}px;top:{n["y"]}px;'
            f'width:{n["w"]}px">{escape(n["text"])}</p>'
        )
    parts.append("</div></div>")
    return "\n".join(parts)


def sections():
    return [
        (SOURCES / f"{key}.html").read_text()
        for key in ["architecture", "cutover", "rollback", "recovery"]
    ]


EXTRA_CSS = (SOURCES / "style.css").read_text()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skill-dir", type=Path, required=True)
    parser.add_argument("--icon-dir", type=Path, required=True)
    args = parser.parse_args()
    skill, icons = args.skill_dir.resolve(), args.icon_dir.resolve()
    template = (skill / "assets" / "template.html").read_text()
    css = re.search(r"<style>([\s\S]*?)</style>", template)[1] + EXTRA_CSS
    diagrams = content()
    OUT.mkdir(parents=True, exist_ok=True)
    head = (SOURCES / "head.html").read_text().replace("%%STYLE%%", css)
    body = []
    for d, explanation in zip(diagrams, sections(), strict=True):
        body.append(
            f'<section class="section" id="section-{d["key"]}"><h2>{escape(d["title"])}</h2>'
            f'<p class="caption">{escape(d["caption"])}</p>{canvas(d)}{explanation}'
            f'<p class="download">이미지: <a href="{d["key"]}.png">밝은 화면 PNG</a> · '
            f'<a href="{d["key"]}-dark.png">어두운 화면 PNG</a></p></section>'
        )
    tail = (SOURCES / "tail.html").read_text()
    source = head + "\n".join(body) + tail
    with tempfile.TemporaryDirectory(prefix="foxtrot-diagrams-") as tmp:
        draft = Path(tmp) / "draft.html"
        draft.write_text(source)
        subprocess.run(
            [
                sys.executable,
                str(skill / "scripts" / "build.py"),
                str(draft),
                str(icons),
                str(OUT / "index.html"),
                "--fonts",
                str(skill / "assets" / "fonts"),
            ],
            check=True,
        )
    (OUT / "model.json").write_text(json.dumps(diagrams, ensure_ascii=False, indent=2) + "\n")
    manifest = json.loads((icons / "manifest.json").read_text())
    used = {n["icon"] for d in diagrams for n in d["nodes"]}
    (OUT / "icon-sources.json").write_text(
        json.dumps({key: manifest[key] for key in sorted(used)}, ensure_ascii=False, indent=2)
        + "\n"
    )
    for name in ["OFL-Geist.txt", "OFL-JetBrainsMono.txt"]:
        shutil.copyfile(skill / "assets" / "fonts" / name, OUT / name)
    shutil.copyfile(skill / "LICENSE", OUT / "STACK_DIAGRAM_LICENSE.txt")
    (OUT / "source-revision.json").write_text(
        json.dumps(
            {
                "repository": "https://github.com/caru-ini/stack-diagram",
                "commit": "75dbd3e3c6fe28844322e6064d3a947b870a6c61",
                "components": ["assets/template.html", "scripts/build.py", "scripts/render_png.py"],
                "implementation": "feat/db-operator working tree, 2026-11-03",
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
