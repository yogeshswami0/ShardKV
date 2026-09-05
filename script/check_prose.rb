#!/usr/bin/env ruby
# frozen_string_literal: true

# Enforces the repo's authoring rules on the tracked tree and on commit
# messages. Run before committing; CI runs the same script.

SELF = "script/check_prose.rb"

# This file necessarily contains the strings it forbids, so it exempts itself.
TRACKED = `git ls-files`.split("\n").reject do |f|
  f == SELF ||
    f.start_with?("build", "docs/images") ||
    f.end_with?(".png", ".jpg", ".jpeg")
end

failures = []

# Dashes. An em-dash or en-dash anywhere, and " -- " used as a substitute for
# one, both read as the same tic.
TRACKED.each do |file|
  next unless File.file?(file)

  begin
    text = File.read(file, encoding: "UTF-8")
    text.valid_encoding?
  rescue StandardError
    next
  end
  next unless text.valid_encoding?

  text.each_line.with_index(1) do |line, n|
    if line.include?("—") || line.include?("–")
      failures << "#{file}:#{n}: em-dash or en-dash"
    end
    if line.include?(" -- ") && !line.start_with?("#!")
      failures << "#{file}:#{n}: ' -- ' used as a dash aside"
    end
  end
end

# Tool attribution. The history is part of what ships, so no assistant name or
# co-author trailer may enter the tree or the log.
FORBIDDEN = [/co-authored-by/i, /\bclaude\b/i, /\bcopilot\b/i, /\bchatgpt\b/i]

TRACKED.each do |file|
  next unless File.file?(file)

  begin
    text = File.read(file, encoding: "UTF-8")
  rescue StandardError
    next
  end
  next unless text.valid_encoding?

  text.each_line.with_index(1) do |line, n|
    FORBIDDEN.each do |pattern|
      failures << "#{file}:#{n}: tool attribution" if line =~ pattern
    end
  end
end

# Commit messages on this branch: one sentence, sentence case, no prefix, no
# trailing period, no body.
base = ENV.fetch("PROSE_BASE", "main")
range = `git rev-parse --verify #{base} 2>/dev/null`.empty? ? nil : "#{base}..HEAD"

if range
  log = `git log --format=%H%x00%s%x00%b%x1e #{range}`
  log.split("\x1e").reject { |e| e.strip.empty? }.each do |entry|
    sha, subject, body = entry.strip.split("\x00")
    next if subject.nil?

    short = sha[0, 8]
    failures << "#{short}: commit body present" unless body.to_s.strip.empty?
    failures << "#{short}: subject ends with a period" if subject.end_with?(".")
    if subject =~ /\A(feat|fix|chore|docs|refactor|test|build|ci)(\(.+\))?:/
      failures << "#{short}: conventional-commit prefix"
    end
    failures << "#{short}: subject over 100 chars" if subject.length > 100
    FORBIDDEN.each do |pattern|
      failures << "#{short}: tool attribution in subject" if subject =~ pattern
    end
  end
end

if failures.empty?
  puts "prose ok: #{TRACKED.length} files"
  exit 0
end

warn "prose check failed:"
failures.each { |f| warn "  #{f}" }
exit 1
