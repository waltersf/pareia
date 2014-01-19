#include "buscabr.h"

// -- retira espaços laterais e espaços duplos ou maiores
std::string trim_reduce(const std::string &str)
{
    // token (what will be removed)
    const std::string &token = " \t";

    // find first and last non token
    const int begin = str.find_first_not_of(token);
    if(begin == std::string::npos) return "";
    const int end = str.find_last_not_of(token);
    // substring it
    std::string toRet = str.substr(begin, end - begin - 1);

    // find first token
    int start = toRet.find_first_of(token);
    while(start != std::string::npos)
    {
        const int finish = toRet.find_first_not_of(token, start);
        const int size = finish - start;

        // replace all consecutive tokens with space
        toRet.replace(start, size, " ");

        start = toRet.find_first_of(token, start + 1);
    }

    return toRet;
}

// -- retira caracteres de pontuação
std::string remove_punctuation(std::string &str)
{
    std::string toRet;
    // remove char if it's a punctuation
    std::remove_copy_if(str.begin(), str.end(),
            std::back_inserter(toRet),
            std::ptr_fun<int, int>(&std::ispunct)
            );

    return toRet;
}

void replace(std::string &str, const std::string &from, const std::string &to)
{
    int start = str.find(from);
    while(start != std::string::npos)
    {
        str.replace(start, from.length(), to);
        start = str.find(from);
    }
}

bool is_after_whitespace(std::string &str, int pos)
{
    if(pos == 0) return true;
    else if(str[pos - 1] == ' ') return true;

    return false;
}

void replace_word_starting_from(std::string &str, const std::string &from, const std::string &to, const int len)
{
    int start = str.find(from);
    while(start != std::string::npos)
    {
        const int prevstart = start;
        if(is_after_whitespace(str, start) || len == 0)
        {
            int end = str.find_first_of(" ", start + 1);

            if(len != 0)
            {
                if(end - start <= from.length() + len || end == -1)
                {
                    if(str[start - 1] == ' ') start--;
                    else end++;

                    str.replace(start, end - start, "");
                }
            } else {
                if(str[start - 1] == ' ') start--;
                else end++;

                str.replace(start, end - start, "");
            }
        }

        start = str.find(from, prevstart + 1);
    }
}

void ltrim(std::string &str, char from)
{
    if(str[0] == from)
    {
        str.replace(0, 1, "");
    }
}

void remove_from_end(std::string &str, const std::string &from)
{
    int start = str.find_first_of(from);
    while(start != std::string::npos)
    {
        const int len = str.length();
        if(start == len - 1) // it's in the end
        {
            str.replace(len - from.length(), from.length(), "");
        } else { // it's in the middle
            if(str[start + 1] == ' ')
            {
                str.replace(start, from.length(), "");
            }
        }

        start = str.find_first_of(from, start + 1);
    }
}

void left_midword_trim(std::string &str, const std::string &from)
{
    int start = str.find_first_of(" ");
    while(start != std::string::npos)
    {
        if(start - 1 > 0)
        {
            for(int i = 0; i < from.length(); i++)
            {
                if(str[start + 1] == from[i])
                {
                    str.replace(start + 1, 1, "");
                    break;
                }
            }
        }

        start = str.find_first_of(" ", start + 1);
    }
}

void first_and_last(std::string &str)
{
    int start = str.find_first_of(" ");
    int end = str.find_last_of(" ");

    if(start == end) return;

    str.replace(start, end - start, "");
}

void tira_particulas(std::string &str)
{
    // p := regexp_replace(p, ' d[aeiour][slr]* ', ' ', modifier => 'i'); -- da/das, do/dos, de/del etc
    replace_word_starting_from(str, "das", "", 1);
    replace_word_starting_from(str, "des", "", 1);
    replace_word_starting_from(str, "dis", "", 1);
    replace_word_starting_from(str, "dos", "", 1);
    replace_word_starting_from(str, "dus", "", 1);
    replace_word_starting_from(str, "drs", "", 1);
    replace_word_starting_from(str, "dal", "", 1);
    replace_word_starting_from(str, "del", "", 1);
    replace_word_starting_from(str, "dil", "", 1);
    replace_word_starting_from(str, "dol", "", 1);
    replace_word_starting_from(str, "dul", "", 1);
    replace_word_starting_from(str, "drl", "", 1);
    replace_word_starting_from(str, "dar", "", 1);
    replace_word_starting_from(str, "der", "", 1);
    replace_word_starting_from(str, "dir", "", 1);
    replace_word_starting_from(str, "dor", "", 1);
    replace_word_starting_from(str, "dur", "", 1);
    replace_word_starting_from(str, "drr", "", 1);
    replace(str, "da", "");
    replace(str, "de", "");
    replace(str, "di", "");
    replace(str, "do", "");
    replace(str, "du", "");
    replace(str, "dr", "");

    // p := regexp_replace(p, ' l[aeiou]s* ', ' ', modifier => 'i');      -- la/le/las/les etc
    replace_word_starting_from(str, "las", "", 1);
    replace_word_starting_from(str, "les", "", 1);
    replace_word_starting_from(str, "lis", "", 1);
    replace_word_starting_from(str, "los", "", 1);
    replace_word_starting_from(str, "lus", "", 1);
    replace(str, "la", "");
    replace(str, "le", "");
    replace(str, "li", "");
    replace(str, "lo", "");
    replace(str, "lu", "");

    // p := regexp_replace(p, ' e ', ' ', modifier => 'i');               -- e isolado
    replace(str, " e ", "");
    // p := regexp_replace(p, ' v[ao]n ', ' ', modifier => 'i');          -- von/van
    replace(str, "von", "");
    replace(str, "van", "");
    // p := regexp_replace(p, ' filho| net+o| sobrinho| junior| jr| segundo| terceiro| primo| bisneto', '',
    //                     position => educacenso_2012.pkg_infra.zvl(instr(p, ' ', -1, 2), 100),
    //                     occurrence => 1, modifier => 'i');
    replace(str, "filho", "");
    replace(str, "neto", "");
    replace(str, "sobrinho", "");
    replace(str, "junior", "");
    replace(str, "jr", "");
    replace(str, "segundo", "");
    replace(str, "terceiro", "");
    replace(str, "primo", "");
    replace(str, "bisneto", "");

    // p := regexp_replace(p, ' filha| net+a| sobrinha| segunda| terceira| prima| bisneta', '',
    //                     position => educacenso_2012.pkg_infra.zvl(instr(p, ' ', -1, 2), 100),
    //                     occurrence => 1, modifier => 'i');
    replace(str, "filha", "");
    replace(str, "neta", "");
    replace(str, "sobrinha", "");
    replace(str, "segunda", "");
    replace(str, "terceira", "");
    replace(str, "prima", "");
    replace(str, "bisneta", "");
}

std::string buscabr(std::string &str, bool firstAndLast) {
    // trim_reduce
    str = trim_reduce(str);

    // p := translate(p, '-', ' ');
    std::replace(str.begin(), str.end(), '-', ' ');

    // p := translate(p, 'a.,;/<>:?¿`''!@#$%&*_', 'a'); -- manter o a para evitar nulo, que dá bobagem no translate
    str = remove_punctuation(str);

    // p := translate(p, c_vogais_acentuadas, c_vogais_planas);
    std::transform(str.begin(), str.end(), str.begin(), ::tolower);

    // p := translate(p, 'ywñ', 'ivn');
    std::replace(str.begin(), str.end(), 'y', 'i');
    std::replace(str.begin(), str.end(), 'w', 'v');
    replace(str, "ñ", "n");

    // p := rtrim(regexp_replace(substr(p,instr(p,' ')+1,length(p)), '[szprmnl]( |$)',' '));
    remove_from_end(str, "s");
    remove_from_end(str, "z");
    remove_from_end(str, "p");
    remove_from_end(str, "r");
    remove_from_end(str, "m");
    remove_from_end(str, "n");
    remove_from_end(str, "l");

    // p := replace(regexp_replace(p, 'qu([ei])', 'k\1'), 'q', 'k'); -- a ordem é muito importante
    replace(str, "que", "ke"); // ta certo isso arnaldo?
    replace(str, "qui", "ki"); // ta certo isso arnaldo?
    std::replace(str.begin(), str.end(), 'q', 'k');

    // p := regexp_replace(replace(replace(p, 'ge', 'je'), 'gi', 'ji'), 'gu([ei])', 'g\1');
    replace(str, "ge", "je");
    replace(str, "gi", "ji");
    replace(str, "gue", "ge"); // ta certo isso arnaldo?
    replace(str, "gui", "gi"); // ta certo isso arnaldo?

    // p := replace(replace(p, 'ce', 'se'), 'ci', 'si');
    replace(str, "ce", "se");
    replace(str, "ci", "si");

    // p := regexp_replace(replace(p, 'ck', 'k'), 'c([aou])', 'k\1');
    replace(str, "ck", "k");
    replace(str, "ca", "ka");
    replace(str, "co", "ko");
    replace(str, "cu", "ku");

    // p := replace(replace(replace(replace(replace(replace(replace(p, 'pr', 'p'), 'pl', 'p'), 'br', 'b'), 'bl', 'b'), 'lh', 'l'), 'nh', 'n'), 'ch', 's');
    replace(str, "pr", "p");
    replace(str, "pl", "p");
    replace(str, "br", "b");
    replace(str, "bl", "b");
    replace(str, "lh", "l");
    replace(str, "nh", "n");
    replace(str, "ch", "s");

    // p := replace(replace(replace(replace(replace(replace(replace(p, 'ct', 't'), 'pt', 't'), 'st', 't'), 'rt', 'p'), 'lt', 't'), 'tr', 't'), 'tl', 't');
    replace(str, "ct", "t");
    replace(str, "pt", "t");
    replace(str, "st", "t");
    replace(str, "rt", "p");
    replace(str, "lt", "t");
    replace(str, "tr", "t");
    replace(str, "tl", "t");

    // p := replace(replace(replace(replace(replace(replace(p, 'mg', 'g'), 'ng', 'g'), 'rg', 'g'), 'lg', 'g'), 'gr', 'g'), 'gl', 'g');
    replace(str, "mg", "g");
    replace(str, "ng", "g");
    replace(str, "rg", "g");
    replace(str, "lg", "g");
    replace(str, "gr", "g");
    replace(str, "gl", "g");

    // p := replace(replace(replace(replace(replace(replace(p, 'md', 'd'), 'nd', 'd'), 'rd', 'd'), 'ld', 'd'), 'dr', 'd'), 'dl', 'd');
    replace(str, "md", "d");
    replace(str, "nd", "d");
    replace(str, "rd", "d");
    replace(str, "ld", "d");
    replace(str, "dr", "d");
    replace(str, "dl", "d");

    // p := replace(replace(replace(replace(replace(replace(p, 'mc', 'k'), 'nc', 'k'), 'rc', 'k'), 'lc', 'k'), 'cr', 'k'), 'cl', 'k');
    replace(str, "mc", "k");
    replace(str, "nc", "k");
    replace(str, "rc", "k");
    replace(str, "lc", "k");
    replace(str, "cr", "k");
    replace(str, "cl", "k");

    // p := replace(replace(replace(replace(p, 'rj', 'j'), 'lj', 'j'), 'mj', 'j'), 'nj', 'j');
    replace(str, "rj", "j");
    replace(str, "lj", "j");
    replace(str, "mj", "j");
    replace(str, "nj", "j");

    // p := replace(replace(replace(replace(p, 'rm', 'm'), 'lm', 'm'), 'gm', 'm'), 'sm', 'm');
    replace(str, "rm", "m");
    replace(str, "lm", "m");
    replace(str, "gm", "m");
    replace(str, "sm", "m");

    // p := replace(replace(replace(replace(p, 'rn', 'n'), 'ln', 'n'), 'gn', 'n'), 'sn', 'n');
    replace(str, "rn", "n");
    replace(str, "ln", "n");
    replace(str, "gn", "n");
    replace(str, "sn", "n");

    // p := replace(replace(replace(p, 'ph', 'f'), 'fr', 'f'), 'fl', 'f');
    replace(str, "ph", "f");
    replace(str, "fr", "f");
    replace(str, "fl", "f");

    // p := replace(replace(p, 'rl', 'l'), 'sl', 'l');
    replace(str, "rl", "l");
    replace(str, "sl", "l");

    // p := replace(replace(p, 'rs', 's'), 'ts', 's');
    replace(str, "rs", "s");
    replace(str, "ts", "s");

    // p := replace(replace(replace(replace(p, 'x', 's'), 'z', 's'), 'ç', 's'), 'c', 's');
    std::replace(str.begin(), str.end(), 'x', 's');
    std::replace(str.begin(), str.end(), 'z', 's');
    replace(str, "ç", "s");
    std::replace(str.begin(), str.end(), 'c', 's');

    // p := regexp_replace(p, '(.)\1+', '\1');
    str.erase(std::unique(str.begin(), str.end()), str.end());

    // p := ltrim(p, 'h');
    ltrim(str, 'h');

    // p := substr(p,1,1)||translate(substr(p,2), '.aeiouh', '.');
    // e vogais e agás exceto a vogal inicial, quando o é, precedida ou não de h - mas o h é eliminado
    left_midword_trim(str, "aeiouh");

    if(firstAndLast) first_and_last(str);

    remove_if(str.begin(), str.end(), isspace);

    return str;
}
