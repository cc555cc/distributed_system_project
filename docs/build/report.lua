-- Pandoc filter that lays out final-report.md like the original Google Doc report.
-- Used with reference.docx; see build.sh.

local PAGE_BREAK = pandoc.RawBlock("openxml", '<w:p><w:r><w:br w:type="page"/></w:r></w:p>')

local function styled(style, blocks)
  return pandoc.Div(blocks, { ["custom-style"] = style })
end

local function is_image_only(block)
  if block.t ~= "Para" then return false end
  local found = false
  for _, inline in ipairs(block.content) do
    if inline.t == "Image" then
      found = true
    elseif inline.t ~= "Space" and inline.t ~= "SoftBreak" then
      return false
    end
  end
  return found
end

-- An italic paragraph that starts with "Figure" or "Table" is a caption.
local function caption_kind(block)
  if block.t ~= "Para" or #block.content ~= 1 or block.content[1].t ~= "Emph" then return nil end
  local first = block.content[1].content[1]
  if first and first.t == "Str" then
    if first.text == "Figure" then return "Image Caption" end
    if first.text == "Table" then return "Table Caption" end
  end
  return nil
end

-- Inline code would render in a coloured monospace font; show it in italics instead.
function Code(code)
  return pandoc.Emph({ pandoc.Str(code.text) })
end

function Pandoc(doc)
  local out = {}
  local on_title_page = true
  for _, block in ipairs(doc.blocks) do
    if on_title_page and block.t == "Header" and pandoc.utils.stringify(block) == "Abstract" then
      on_title_page = false
      table.insert(out, PAGE_BREAK)
    end

    if block.t == "Header" and pandoc.utils.stringify(block):match("^Appendix A") then
      table.insert(out, PAGE_BREAK)
    end

    if on_title_page then
      if is_image_only(block) then
        table.insert(out, styled("Centered Image", { block }))
      elseif block.t == "Header" then
        table.insert(out, styled("Report Title", { pandoc.Para(block.content) }))
      elseif block.t == "Para" and pandoc.utils.stringify(block):match("Group %d+") then
        table.insert(out, styled("Title Page Bold", { block }))
      elseif block.t == "Para" then
        table.insert(out, styled("Title Page", { block }))
      else
        table.insert(out, block)
      end
    elseif is_image_only(block) then
      table.insert(out, styled("Centered Image", { block }))
    else
      local kind = caption_kind(block)
      if kind then
        -- The caption style supplies the italics, so unwrap the emphasis.
        table.insert(out, styled(kind, { pandoc.Para(block.content[1].content) }))
      else
        table.insert(out, block)
      end
    end
  end
  doc.blocks = out
  return doc
end
