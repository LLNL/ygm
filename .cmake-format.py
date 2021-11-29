# -----------------------------
# Options effecting formatting.
#
# Requires cmake-format (pip install cmake_format). See
# https://cmake-format.readthedocs.io/en/latest/configuration.html for more
# examples.
# -----------------------------
with section("format"):

    # How wide to allow formatted cmake files
    line_width = 80

    # How many spaces to tab for indent
    tab_size = 4

    use_tabchars = False

    # If true, separate flow control names from their parentheses with a space
    separate_ctrl_name_with_space = True

    # If true, separate function names from parentheses with a space
    separate_fn_name_with_space = False

    # If a statement is wrapped to more than one line, than dangle the closing
    # parenthesis on its own line.
    dangle_parens = True

    # If the trailing parenthesis must be 'dangled' on its on line, then align it
    # to this reference: `prefix`: the start of the statement,  `prefix-indent`:
    # the start of the statement, plus one indentation  level, `child`: align to
    # the column of the arguments
    dangle_align = "prefix"

    # Format command names consistently as 'lower' or 'upper' case
    command_case = "canonical"

    # Format keywords consistently as 'lower' or 'upper' case
    keyword_case = "unchanged"
