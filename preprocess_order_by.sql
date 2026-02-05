create schema sql_preprocessing;

--/
CREATE or replace SCRIPT sql_preprocessing.test_preprocess() AS
      import('sql_preprocessing.functions', 'functions')

  -- main entry point for preprocessor: fetch current SQL, transform, and set it
  local current = sqlparsing.getsqltext()
  local transformed = functions.add_order_by_for_bind(current)
  sqlparsing.setsqltext(transformed)

/


--/
CREATE or replace SCRIPT sql_preprocessing.functions() AS
function add_order_by_for_bind(sqltext)

    print('--- preprocess() entered ---')
    print('Original SQL:')
    print(sqltext)

    -- Tokenize input
    local tokens = sqlparsing.tokenize(sqltext)
    if not tokens or #tokens == 0 then
        print('Tokenization failed or returned empty token list')
        return sqltext
    end
    print('Tokenization successful; total tokens: ' .. tostring(#tokens))

    ------------------------------------------------------------------------
    -- Find last "( SELECT" occurrence (ignoring whitespace/comments)
    ------------------------------------------------------------------------
    local last_open_pos = nil
    local search_pos = 1
    local match_count = 0

    while true do
        local found = sqlparsing.find(
            tokens,
            search_pos,
            true,   -- searchSameLevel
            false,  -- exact sequence
            sqlparsing.iswhitespaceorcomment,
            '(', 'SELECT'
        )

        if found == nil then
            break
        end

        match_count = match_count + 1
        last_open_pos = found[1]
        search_pos = last_open_pos + 1

        print('Found "( SELECT" match #' .. tostring(match_count) ..
              ' at token index ' .. tostring(last_open_pos))
    end

    if last_open_pos == nil then
        print('No "( SELECT" pattern found ? nothing to rewrite')
        return sqltext
    end
    print('Using last "( SELECT" at token index ' .. tostring(last_open_pos))

    ------------------------------------------------------------------------
    -- Robust parenthesis matching (manual depth counter)
    -- This replaces the previous sqlparsing.find(...) for ')' which could fail.
    ------------------------------------------------------------------------
    local close_pos = nil
    local depth = 0
    for i = last_open_pos + 1, #tokens do
        local tk = tokens[i]
        if tk == '(' then
            depth = depth + 1
            -- debug:
            print('  saw "(" at token ' .. tostring(i) .. ' ? depth now ' .. tostring(depth))
        elseif tk == ')' then
            if depth == 0 then
                close_pos = i
                print('Matching ")" for chosen "(" found at token index ' .. tostring(close_pos))
                break
            else
                depth = depth - 1
                print('  saw ")" at token ' .. tostring(i) .. ' ? depth now ' .. tostring(depth))
            end
        end
    end

    if close_pos == nil then
        print('ERROR: Could not find matching ")" for subselect (manual scan failed)')
        -- Optionally dump a compact token window for debugging
        local start_snip = math.max(1, last_open_pos - 5)
        local end_snip = math.min(#tokens, last_open_pos + 20)
        print('Tokens near last "(" (index ' .. tostring(last_open_pos) .. '):')
        for j = start_snip, end_snip do
            print('  [' .. tostring(j) .. '] -> ' .. tostring(tokens[j]))
        end
        return sqltext
    end

    ------------------------------------------------------------------------
    -- Scan tokens inside the chosen subselect for '?' (bind parameter)
    ------------------------------------------------------------------------
    local has_bind = false
    for i = last_open_pos + 1, close_pos - 1 do
        if tokens[i] == '?' then
            has_bind = true
            print('Bind parameter "?" found at token index ' .. tostring(i))
            break
        end
    end

    if not has_bind then
        print('No bind parameter "?" found inside subselect ? no rewrite')
        return sqltext
    end

    ------------------------------------------------------------------------
    -- Check if an ORDER BY exists inside that subselect
    ------------------------------------------------------------------------
    local orderby = sqlparsing.find(
        tokens,
        last_open_pos + 1,
        true,
        true,
        sqlparsing.iswhitespaceorcomment,
        'ORDER', 'BY'
    )
    if orderby ~= nil then
        print('ORDER BY already exists inside subselect at token index ' ..
              tostring(orderby[1]) .. ' ? no rewrite')
        return sqltext
    end
    print('No ORDER BY found inside subselect')

    ------------------------------------------------------------------------
    -- Insert " ORDER BY FALSE " before the closing ) and return new SQL
    ------------------------------------------------------------------------
    print('All conditions satisfied ? inserting "ORDER BY FALSE"')
    local before = table.concat(tokens, '', 1, close_pos - 1)
    local after  = table.concat(tokens, '', close_pos) -- includes ')' and rest
    local new_sql = before .. ' ORDER BY FALSE ' .. after

    print('Rewritten SQL:')
    print(new_sql)
    print('--- preprocess() finished ---')
    return new_sql
end
/
