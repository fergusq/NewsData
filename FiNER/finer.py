import os
import re
import hfst
import omorfi_postag

class Finer:
    """
    Do Finnish named entity recognition using FinnPos (a dependency), FiNER
    and HFST.
    """
    def __init__(self, datadir):
        """
        The compulsory argument *datadir* should be a path to eg. the /tag/
        directory of a finnish-tagtools package.
        """
        self.datadir = datadir
        self.postagger = omorfi_postag.TextTagger(self.datadir)
        self.p1_tagger = hfst.PmatchContainer(self.datadir + "/proper_tagger_ph1.pmatch")
        self.p2_tagger = hfst.PmatchContainer(self.datadir + "/proper_tagger_ph2.pmatch")

        self.subs = [
            ("ntelu#", "nnella#"), 
            ("ntely#", "nnellä#"), 
            ("ltelu#", "llella#"), 
            ("ltely#", "llellä#"), 
            ("rtelu#", "rrella#"), 
            ("rtely#", "rrellä#"), 
            ("ppelu#", "pella#"), 
            ("ppely#", "pellä#"), 
            ("ttelu#", "tella#"), 
            ("ttely#", "tellä#"), 
            ("kkelu#", "kella#"), 
            ("kkely#", "kellä#"), 
            ("tely#", "dellä#"), 
            ("telu#", "della#"), 
            ("kelu#", "ella#"), 
            ("kely#", "ellä#"), 
            ("elu#", "ella#"), 
            ("ely#", "ellä#"), 
            ("ilu#", "illa#"), 
            ("ily#", "illä#"), 
            ("ltaamis#", "llata#"), 
            ("ltäämis#", "llätä#"), 
            ("bbaamis#", "bata#"), 
            ("bbäämis#", "bätä#"), 
            ("ggaamis#", "gata#"), 
            ("ggäämis#", "gätä#"), 
            ("ppaamis#", "pata#"), 
            ("ppäämis#", "pätä#"), 
            ("ttaamis#", "tata#"), 
            ("ttäämis#", "tätä#"), 
            ("ppaamis#", "kata#"), 
            ("ppäämis#", "kätä#"), 
            ("paamis#", "vata#"), 
            ("päämis#", "vätä#"), 
            ("toamis#", "dota#"), 
            ("taamis#", "data#"), 
            ("täämis#", "dätä#"),
            ("koamis#", "ota#"), 
            ("kaamis#", "ata#"), 
            ("käämis#", "ätä#"), 
            ("lkenemis#", "ljeta#"), 
            ("kenemis#", "eta#"), 
            ("enemis#", "eta#"), 
            ("enemis#", "etä#"), 
            ("itsemis#", "ita#"), 
            ("itsemis#", "itä#"), 
            ("amis#", "ta#"),
            ("ämis#", "tä#"),
            ("kemis#", "hdä#"),
            ("mis#", "da#"),
            ("mis#", "dä#"),
            ("mis#", "a#"),
            ("mis#", "ä#"),
            ("is#", "inen#"),
            ("s#", "nen#"),
            ("s#", "kset#"),
            ("uden#", "us#"),
            ("yden#", "ys#"),
            # Pluralized lemmas
            ("kulu#", "kulut#"),
            ("olo#", "olot#"),
            ("tila#", "tilat#"),
            ("kilpailu#", "kilpailut#"),
            ("kisa#", "kisat#"),
            ("saksi#", "sakset#"),
            ("hää#", "häät#"),
            ("juhla#", "juhlat#"),
            ("housu#", "housut#"),
            ("hius#", "hiukset#"),
            ("markkina#", "markkinat#"),
            ("päivä#", "päivät#"),
            ("suhde#", "suhteet#"),
            ("resurssi#", "resurssit#"),
            ("voima#", "voimat#"),
            ("kasvo#", "kasvot#"),
            ("lasi#", "lasit#"),
            ("tieto#", "tiedot#"),
        ]
        
        self.regex_filename = self.datadir + 'lemma-errors.tsv'
        self.regexes = []
        for line in open(self.regex_filename, 'r'):
            w_patt, l_patt, l_new = line.strip().split('\t')
            self.regexes.append((re.compile(w_patt + '.*'), re.compile(l_patt+'\\Z'), l_new))

        self.open_and_close_tag_re = re.compile(r'<((Enamex|Timex|Numex|Exc)[^>]+)>(.+)</\1>')
        self.open_and_close_tag_re_replacement = r'\3<\1/>'
        self.open_tag_re = re.compile(r'^(<(Enamex|Timex|Numex|Exc)[^>]+>)([^\t].*)$')
        self.open_tag_re_replacement = r'\3\1'
        self.nested_tag_4 = re.compile(r'(</?(Enamex|Timex|Numex)[^>]+4/?>)([^\t]*\t[^\t]*\t[^\t]*\t)')
        self.nested_tag_3 = re.compile(r'(</?(Enamex|Timex|Numex)[^>]+3/?>)([^\t]*\t[^\t]*\t)')
        self.nested_tag_2 = re.compile(r'(</?(Enamex|Timex|Numex)[^>]+2/?>)([^\t]*\t)')
        self.nested_tag_1 = re.compile(r'\t+(<(Enamex|Timex|Numex)[^>]+1>)')
        self.nested_tag_1_replacement = r'\t\1'
        self.nested_tags = re.compile(r'(</?(Enamex|Timex|Numex)[^>1234]+)[1234](/?>)')
        self.nested_tags_replacement = r'\1\3'
        self.exc_tag_re = re.compile(r'</?Exc[^>]+>')

    def format_for_nertag(self, sentences):
        def format_token(token):
            surface, lemma, morph, sem = token
            morph = morph.replace('|', '')
            sem = sem.replace('|', '')
            return (surface, lemma.lower(), morph, sem)
        return [[format_token(token) for token in sentence] for sentence in sentences]

    def normalize_lemmas(self, sentences):
        # - Correct frequent erronoeus lemmas
        # - Replace hashes marking morpheme boundaries (#) with hyphens in lemma forms whenever necessary
        # ( Otherwise remove hashes in lemma forms )

        def inf2prefix(wform, lemma_new):
            for (w_end, l_end) in self.subs:
                if wform.lower().startswith(lemma_new.replace(l_end, w_end[:-1])):
                    lemma_new = lemma_new.replace(l_end, w_end)
                    break
            return lemma_new

        def fix_nouns(wform, lemma_new):
            for (w_regex, l_regex, l_new) in self.regexes:
                if re.search(l_regex, lemma_new) != None:
                    if re.fullmatch(w_regex, wform.lower()):
                        lemma_new = re.sub(l_regex, l_new, lemma_new)
            return lemma_new

        def correct(token):
            wform, lemma, morph, semtag = token
            lemma_new = ''
            lemma = lemma.replace('#-', '#')
            lemma = lemma.replace('#', '#|')
            if wform.startswith('-') == True and lemma.startswith('-') == False:
                lemma = '-'+lemma
            for m in lemma.split('|'):
                lemma_new = lemma_new + m

                if wform.lower().startswith( lemma_new[:-1] ) == False:
                    lemma_new = inf2prefix(wform, lemma_new)

                if wform.lower().startswith(lemma_new.replace('-#', '-')):
                    lemma_new = lemma_new.replace('-#', '-')

                if wform.lower().startswith(lemma_new.replace('#', '-')):
                    lemma_new = lemma_new.replace('#', '-')

                if wform.lower().startswith(lemma_new.replace('-#', '')):
                    lemma_new = lemma_new.replace('-#', '')
            
                lemma_new = lemma_new.rstrip('#')
            lemma_new = fix_nouns(wform, lemma_new)

            # Restore hyphens removed by OMorFi and FinnPOS
            if '-' in wform and '-' not in lemma_new:
                pfx = wform.lower().split('-')[0]
                if lemma_new.startswith(pfx):
                    lemma_new = pfx + '-' + lemma_new[len(pfx):]
    
            return((wform, lemma_new, morph, semtag))
    
        return [[correct(token) for token in sentence] for sentence in sentences]

    def prefilt_tags(self, sentences):
        # 1) Do some unification to the Omorfi tagging
        #  a) Add missing NUM & CASE tags
        # 2) Fix defective guessed lemmas
        # 3) Correct some >90% sure tagging errors
        #  a) 'Juhani' != Juha+PX
        #  b) 'Mari(tt)a' != Mari+PAR/ABE
        #  c) 'Kansa' != 'Ka'
        #  d) 'Line' != 'Li'
        #  e) 'noin' != 'noki'
        def handle_token(token):
            surface, lemma, morph, sem = token
            if '[POS=NOUN]' in morph:
                if '[NUM=' not in morph:
                    morph += '[NUM=SG]'
                if '[CASE=' not in morph:
                    morph += '[CASE=NOM]'
            if surface == 'Juhani' and lemma == 'juha' and '[POSS=SG1]' in morph:
                lemma = 'juhani'
                idx = morpho.index('[CASE=')
                morpho = morpho[:idx] + '[CASE=NOM]'
            return (surface, lemma, morph, sem)
        # -e 's/^Maria\t[^\t]+\t(.*)\[CASE=PAR\]/Maria\tMaria\t\1[CASE=NOM]/' \
        # -e 's/^Maritta\t[^\t]+\t(.*)\[CASE=ABE\]/Maritta\tMaritta\t\1[CASE=NOM]/' \
        # -e 's/^Kansa\tKa\t(.*)\[CASE=...\]\[POSS=3\]/Kansa\tKansa\t\1[CASE=NOM]/' \
        # -e 's/^Line\tLi\t(.*)\[CASE=COM\]/Line\tLine\t\1[CASE=NOM]/' \
        # -e 's/^([Nn])oin\tnoki\t.*\t/\1oin\tnoin\t[POS=PARTICLE][SUBCAT=ADVERB]\t/' \
        # -e 's/.$/&\t/'

        return [[handle_token(token) for token in sentence] for sentence in sentences]

    def add_boundaries(self, sentences):
        retval = ''
        for sentence in sentences:
            for token in sentence:
                retval += '\t'.join(token) + '\t\n'
            retval += '.#.\n'
        return retval[:-4]

    def proper_tag1(self, s):
        return self.p1_tagger.match(s)

    def proper_tag2(self, s):
        return self.p2_tagger.match(s)

    def move_tags(self, s):
        # Move start tags from beginning of each line to their respective columns
        # Tags with names ending in 1, 2, 3, or 4 are moved to columns 5, 6, 7, and 8 respectively
        # The numbers denote nesting depth and are ultimately removed
        retval = ''
        for line in s.split('\n'):
            if line == '.#.':
                retval += line + '\n'
                continue
            line = re.sub(self.open_and_close_tag_re, self.open_and_close_tag_re_replacement, line)
            line = re.sub(self.open_tag_re, self.open_tag_re_replacement, line)
            fields = line.count('\t') + 1
            if fields < 8:
                line = line + (8 - fields) * '\t'
            line = re.sub(self.nested_tag_4, self.open_tag_re_replacement, line)
            line = re.sub(self.nested_tag_3, self.open_tag_re_replacement, line)
            line = re.sub(self.nested_tag_2, self.open_tag_re_replacement, line)
            line = re.sub(self.nested_tag_1, self.nested_tag_1_replacement, line)
            line = re.sub(self.nested_tags, self.nested_tags_replacement, line)
            retval += line + '\n'
        return retval

    def remove_exc(self, s):
        # Remove excess empty lines
        # Remove ".#." strings marking sentence boundaries
        # Remove <Exc___>...</Exc___> tags
        retval = ''
        for line in s.split('\n'):
            if line.strip() == '':
                continue
            if line.strip() == '.#.':
                retval += '\n'
                continue
            line = re.sub(self.exc_tag_re, '', line)
            retval += line + '\n'
        return retval

    def __call__(self, text):
        """
        Takes running (raw) text, returns list of sentences, each of which
        is a list of token-nertag pairs.
        """
        pipeline = [self.postagger,
                    self.format_for_nertag,
                    self.normalize_lemmas,
                    self.prefilt_tags,
                    self.add_boundaries,
                    self.proper_tag1,
                    self.move_tags,
                    self.proper_tag1,
                    self.move_tags,
                    self.remove_exc]
        for function in pipeline:
            text = function(text)
        sentences = []
        sentence = []
        for line in text.split('\n'):
            if line == '':
                sentences.append(sentence)
                sentence = []
                continue
            parts = line.split('\t')
            sentence.append(parts)
        return sentences
