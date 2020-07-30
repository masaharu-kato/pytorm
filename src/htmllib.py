from typing import Any, Callable, Dict, Iterable, Iterator, List, IO, Union, Optional
import html
from abc import ABCMeta, abstractclassmethod
# from dataclasses import dataclass


class Stream(metaclass=ABCMeta):
    def __init__(self, out_stream:IO):
        self.out_stream = out_stream

    def output(self, text:str, *, end:str='\n'):
        print(text, end=end, file=self.out_stream)

    def output_in_line(self, text:str):
        self.output(text, end='')

    def output_in_line_end(self, text:str):
        self.output(text)

    @abstractclassmethod
    def add_indent(self, lv:int=1): pasmn.s

    @abstractclassmethod
    def sub_indent(self, lv:int=1): pass


class FormattedStream(Stream):
    def __init__(self, out_stream:IO, *, indent_str:str='  ', indent_level:int=0):
        super().__init__(out_stream)
        self.indent_str = indent_str
        self.indent_level = indent_level

    def add_indent(self, lv:int=1):
        self.indent_level = max(self.indent_level + lv, 0) 

    def sub_indent(self, lv:int=1):
        self.add_indent(-lv)

    def output(self, text:str, *, end:str='\n'):
        super().output(self.indent_str * self.indent_level + text, end=end)

    def output_in_line(self, text:str):
        super().output(text, end='')

    def output_in_line_end(self, text:str):
        super().output(text)


class Page:
    def __init__(self, stream:Stream):
        self.stream = stream

        self.closed = False


    def element(self,
        tag_name:str,
        attrs:Dict[str, str] = {},
        contents:Optional[Iterable] = None,
        **options
    ):
        if self.closed: raise RuntimeError('FlowElement already closed.')
        return Element(self, tag_name, attrs, contents, **options)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return self

    def close(self):
        if self.closed: raise RuntimeError('FlowElement already closed.')
        self.closed = True

    def text(self, text:str):
        self.output(html.escape(text))

    def text_in_line(self, text:str):
        self.output_in_line(html.escape(text))

    def doctype(self):
        self.stream.output('<!DOCTYPE html>')

    def html(self, *contents, **attrs):
        return Html(self, attrs, contents)

    def output(self, text:str, *, end:str='\n'):
        if self.closed: raise RuntimeError('FlowElement already closed.')
        return self.stream.output(text, end=end)

    def output_in_line(self, text:str):
        if self.closed: raise RuntimeError('FlowElement already closed.')
        return self.stream.output_in_line(text)

    def output_in_line_end(self, text:str):
        if self.closed: raise RuntimeError('FlowElement already closed.')
        return self.stream.output_in_line_end(text)


class Element(Page):
    def __init__(self,
        parent:Page,
        tag_name:str,
        attrs:Dict[str, Any] = {},
        contents:Any = None,
        *,
        self_close:bool=False,
    ):
        self.stream = parent.stream
        self.tag_name = html.escape(tag_name)
        
        self.closed = False

        self.output(
            '<' + self.tag_name + ''.join(
                ' ' + html.escape(k.strip('_')) + '="' + html.escape(str(v), quote=True) + '"' for k, v in attrs.items() if v is not None
            ) + (' /' if self_close else '') +  '>',
            end = ('' if contents else '\n')
        )

        if self_close:
            return

        if contents:

            if isinstance(contents, str):
                self.text_in_line(contents)
            elif isinstance(contents, Iterable):
                self.text_in_line(' '.join(map(str, contents)))
            else:
                self.text_in_line(str(contents))

            self.output_in_line_end('</' + self.tag_name + '>')
            self.closed = True
            return

        self.stream.add_indent()


    def close(self):
        if self.closed:
            raise RuntimeError('FlowElement already closed.')
        self.stream.sub_indent()
        self.output('</' + self.tag_name + '>')
        self.closed = True



OptStr = Optional[str]



class Html(Element):
    def __init__(self, parent, attrs, contents):
        super().__init__(parent, 'html', attrs, contents)

    def head(self, *contents, **attrs):
        return Head(self, attrs, contents)

    def body(self, *contents, **attrs):
        return FlowElement(self, 'body', attrs, contents)

class Head(Element):
    def __init__(self, parent, attrs, contents):
        super().__init__(parent, 'head', attrs, contents)

    def meta(self, **attrs):
        return Meta(self, attrs)

    def title(self, *contents, **attrs):
        return Title(self, attrs, contents)

class Meta(Element):
    def __init__(self, parent, attrs):
        super().__init__(parent, 'meta', attrs, self_close=True)

class Title(Element):
    def __init__(self, parent, attrs, content):
        super().__init__(parent, 'title', attrs, content)

class FlowElement(Element):
    def __init__(self, parent, tag_name:str, attrs, contents):
        super().__init__(parent, tag_name, attrs, contents)

    def div(self, *contents, **attrs):
        return FlowElement(self, 'div', attrs, contents)

    def span(self, *contents, **attrs):
        return FlowElement(self, 'span', attrs, contents)

    def p(self, *contents, **attrs):
        return FlowElement(self, 'p', attrs, contents)

    def table(self, *contents, **attrs):
        return Table(self, attrs, contents)

    def div_class(self, _class, *contents, **attrs):
        """ Alias of `self.div(content, ..., _class='classname', ...)` """
        return FlowElement(self, 'div', {**attrs, 'class':_class}, contents)

    def span_class(self, _class, *contents, **attrs):
        return FlowElement(self, 'span', {**attrs, 'class':_class}, contents)

    def p_class(self, _class, *contents, **attrs):
        return FlowElement(self, 'p', {**attrs, 'class':_class}, contents)


    def area  (self, **attrs): return EmptyElement(self, 'area'  , attrs)
    def br    (self, **attrs): return EmptyElement(self, 'br'    , attrs)
    def embed (self, **attrs): return EmptyElement(self, 'embed' , attrs)
    def hr    (self, **attrs): return EmptyElement(self, 'hr'    , attrs)
    
    def audio   (self, *contents, **attrs): return Audio   (self, attrs, contents)
    def button  (self, *contents, **attrs): return Button  (self, attrs, contents)
    def canvas  (self, *contents, **attrs): return Canvas  (self, attrs, contents)
    def data    (self, *contents, **attrs): return Data    (self, attrs, contents)
    def details (self, *contents, **attrs): return Details (self, attrs, contents)
    def dl      (self, *contents, **attrs): return DL      (self, attrs, contents)
    def figure  (self, *contents, **attrs): return Figure  (self, attrs, contents)
    def form    (self, *contents, **attrs): return Form    (self, attrs, contents)
    def _object (self, *contents, **attrs): return Object  (self, attrs, contents)
    def picture (self, *contents, **attrs): return Picture (self, attrs, contents)
    def select  (self, *contents, **attrs): return Select  (self, attrs, contents)
    def textarea(self, *contents, **attrs): return TextArea(self, attrs, contents)
    def time    (self, *contents, **attrs): return Time    (self, attrs, contents)
    def video   (self, *contents, **attrs): return Video   (self, attrs, contents)

    def img  (self, **attrs): return Img  (self, attrs)
    def input(self, **attrs): return Input(self, attrs)


    def bdi (self, *contents, **attrs): return PhraseElement(self, 'bdi' , attrs, contents)
    def bdo (self, *contents, **attrs): return PhraseElement(self, 'bdo' , attrs, contents)
    def var (self, *contents, **attrs): return PhraseElement(self, 'var' , attrs, contents)
    def kbd (self, *contents, **attrs): return PhraseElement(self, 'kbd' , attrs, contents)
    def dfn (self, *contents, **attrs): return PhraseElement(self, 'dfn' , attrs, contents)
    def cite(self, *contents, **attrs): return PhraseElement(self, 'cite', attrs, contents)
    def code(self, *contents, **attrs): return PhraseElement(self, 'code', attrs, contents)

    def a         (self, *contents, **attrs): return FlowElement(self, 'a'         , attrs, contents)
    def abbr      (self, *contents, **attrs): return FlowElement(self, 'abbr'      , attrs, contents)
    def address   (self, *contents, **attrs): return FlowElement(self, 'address'   , attrs, contents)
    def article   (self, *contents, **attrs): return FlowElement(self, 'article'   , attrs, contents)
    def aside     (self, *contents, **attrs): return FlowElement(self, 'aside'     , attrs, contents)
    def b         (self, *contents, **attrs): return FlowElement(self, 'b'         , attrs, contents)  
    def blockquote(self, *contents, **attrs): return FlowElement(self, 'blockquote', attrs, contents)
    def em        (self, *contents, **attrs): return FlowElement(self, 'em'        , attrs, contents)
    def footer    (self, *contents, **attrs): return FlowElement(self, 'footer'    , attrs, contents)
    def ol        (self, *contents, **attrs): return FlowElement(self, 'ol'        , attrs, contents)
    def ul        (self, *contents, **attrs): return FlowElement(self, 'ul'        , attrs, contents)
    def h1        (self, *contents, **attrs): return FlowElement(self, 'h1'        , attrs, contents)
    def h2        (self, *contents, **attrs): return FlowElement(self, 'h2'        , attrs, contents)
    def h3        (self, *contents, **attrs): return FlowElement(self, 'h3'        , attrs, contents)
    def h4        (self, *contents, **attrs): return FlowElement(self, 'h4'        , attrs, contents)
    def h5        (self, *contents, **attrs): return FlowElement(self, 'h5'        , attrs, contents)
    def h6        (self, *contents, **attrs): return FlowElement(self, 'h6'        , attrs, contents)
    def header    (self, *contents, **attrs): return FlowElement(self, 'header'    , attrs, contents)
    def i         (self, *contents, **attrs): return FlowElement(self, 'i'         , attrs, contents)
    def ins       (self, *contents, **attrs): return FlowElement(self, 'ins'       , attrs, contents)
    def main      (self, *contents, **attrs): return FlowElement(self, 'main'      , attrs, contents)
    def map       (self, *contents, **attrs): return FlowElement(self, 'map'       , attrs, contents)
    def mark      (self, *contents, **attrs): return FlowElement(self, 'mark'      , attrs, contents)
    def math      (self, *contents, **attrs): return FlowElement(self, 'math'      , attrs, contents)
    def menu      (self, *contents, **attrs): return FlowElement(self, 'menu'      , attrs, contents)
    def nav       (self, *contents, **attrs): return FlowElement(self, 'nav'       , attrs, contents)
    def _output   (self, *contents, **attrs): return FlowElement(self, '_output'   , attrs, contents)
    def pre       (self, *contents, **attrs): return FlowElement(self, 'pre'       , attrs, contents)
    def q         (self, *contents, **attrs): return FlowElement(self, 'q'         , attrs, contents)
    def ruby      (self, *contents, **attrs): return FlowElement(self, 'ruby'      , attrs, contents)
    def s         (self, *contents, **attrs): return FlowElement(self, 's'         , attrs, contents) 
    def samp      (self, *contents, **attrs): return FlowElement(self, 'samp'      , attrs, contents)
    def section   (self, *contents, **attrs): return FlowElement(self, 'section'   , attrs, contents)
    def small     (self, *contents, **attrs): return FlowElement(self, 'small'     , attrs, contents)
    def sub       (self, *contents, **attrs): return FlowElement(self, 'sub'       , attrs, contents)
    def sup       (self, *contents, **attrs): return FlowElement(self, 'sup'       , attrs, contents)
    def wbr       (self, *contents, **attrs): return FlowElement(self, 'wbr'       , attrs, contents)
    def _del      (self, *contents, **attrs): return FlowElement(self, 'del'       , attrs, contents)


# ---- Base element classes ----

class EmptyElement(Element):
    def __init__(self, parent, tag_name:str, attrs):
        super().__init__(parent, 'table', attrs, self_close=True)


class PhraseElement(Element):
    pass


class TextOnlyElement(Element):
    pass

# ---- Table tags ----

class Table(Element):
    def __init__(self, parent, attrs, contents):
        super().__init__(parent, 'table', attrs, contents)

    def caption(self, *contents, **attrs):
        return Element(self, 'caption', attrs, contents)

    def colgroup(self, *contents, **attrs):
        return ColGroup(self, attrs, contents)

    def thead(self, *contents, **attrs):
        return THead(self, attrs, contents)

    def tbody(self, *contents, **attrs):
        return TBody(self, attrs, contents)

    def tfoot(self, *contents, **attrs):
        return TFoot(self, attrs, contents)

    def tr(self, *contents, **attrs):
        return TR(self, attrs, contents)

    def tr_class(self, _class, *contents, **attrs):
        return TR(self, {**attrs, 'class':_class}, contents)

    # def cells(self,
    #     data: Iterable[Iterable],
    #     gen_cell_v    : Optional[Callable[[int, int, Any], Any]] = None,
    #     gen_cell_a    : Optional[Callable[[int, int, Any], Dict[str, Any]]] = None,
    #     gen_row_a     : Optional[Callable[[int], Dict[str, Any]]] = None,
    #     head_row_v    : Optional[Callable[[int], Any]] = None,
    #     head_row_a    : Optional[Callable[[int], Dict[str, Any]]] = None,
    #     left_column_v : Optional[Callable[[int], Any]] = None,
    #     left_column_a : Optional[Callable[[int], Dict[str, Any]]] = None,
    #     foot_row_v    : Optional[Callable[[int], Any]] = None,
    #     foot_row_a    : Optional[Callable[[int], Dict[str, Any]]] = None,
    #     right_column_v: Optional[Callable[[int], Any]] = None,
    #     right_column_a: Optional[Callable[[int], Dict[str, Any]]] = None,
    # ):
        




class ColGroup(Element):
    def __init__(self, parent, attrs, contents):
        super().__init__(parent, 'colgroup', attrs, contents)

    def col(self, *contents, **attrs):
        return Element(self, 'col', attrs, contents, self_close=True)

class THead(Element):
    def __init__(self, parent, attrs, contents):
        super().__init__(parent, 'thead', attrs, contents)

    def tr(self, *contents, **attrs):
        return TR(self, attrs, contents)

    def tr_class(self, _class, *contents, **attrs):
        return TR(self, {**attrs, 'class':_class}, contents)

class TBody(Element):
    def __init__(self, parent, attrs, contents):
        super().__init__(parent, 'tbody', attrs, contents)

    def tr(self, *contents, **attrs):
        return TR(self, attrs, contents)

    def tr_class(self, _class, *contents, **attrs):
        return TR(self, {**attrs, 'class':_class}, contents)

class TFoot(Element):
    def __init__(self, parent, attrs, contents):
        super().__init__(parent, 'tfoot', attrs, contents)

    def tr(self, *contents, **attrs):
        return TR(self, attrs, contents)

    def tr_class(self, _class, *contents, **attrs):
        return TR(self, {**attrs, 'class':_class}, contents)

class TR(Element):
    def __init__(self, parent, attrs, contents):
        super().__init__(parent, 'tr', attrs, contents)

    def td(self, *contents, **attrs):
        return TD(self, attrs, contents)

    def th(self, *contents, **attrs):
        return TH(self, attrs, contents)

    def td_class(self, _class, *contents, **attrs):
        return TD(self, {**attrs, 'class':_class}, contents)

    def th_class(self, _class, *contents, **attrs):
        return TH(self, {**attrs, 'class':_class}, contents)


class TD(FlowElement):
    def __init__(self, parent, attrs, contents):
        super().__init__(parent, 'td', attrs, contents)

class TH(FlowElement):
    def __init__(self, parent, attrs, contents):
        super().__init__(parent, 'th', attrs, contents)


# ---- Definition list tags ----

class DL(FlowElement):
    def __init__(self, parent, attrs, contents):
        super().__init__(parent, 'dl', attrs, contents)

    def dt(self, *contents, **attrs):
        return DT(self, attrs, contents)

    def dd(self, *contents, **attrs):
        return DD(self, attrs, contents)

    def dt_class(self, _class, *contents, **attrs):
        return DT(self, {**attrs, 'class':_class}, contents)

    def dd_class(self, _class, *contents, **attrs):
        return DD(self, {**attrs, 'class':_class}, contents)

class DT(FlowElement):
    def __init__(self, parent, attrs, contents):
        super().__init__(parent, 'dt', attrs, contents)

class DD(FlowElement):
    def __init__(self, parent, attrs, contents):
        super().__init__(parent, 'dd', attrs, contents)


# ---- Media tags ----

class Audio(PhraseElement):
    def __init__(self, parent, attrs, contents):
        super().__init__(parent, 'audio', attrs, contents)

    def source(self, **attrs):
        return EmptyElement(self, 'source', attrs)

    def track(self, **attrs):
        return EmptyElement(self, 'track', attrs)

class Video(PhraseElement):
    def __init__(self, parent, attrs, contents):
        super().__init__(parent, 'video', attrs, contents)

    def source(self, **attrs):
        return EmptyElement(self, 'source', attrs)

    def track(self, **attrs):
        return EmptyElement(self, 'track', attrs)

class Picture(PhraseElement):
    def __init__(self, parent, attrs, contents):
        super().__init__(parent, 'picture', attrs)

    def img(self, **attrs):
        return Img(self, attrs)

    def source(self, **attrs):
        return EmptyElement(self, 'source', attrs)

class Figure(PhraseElement):
    def __init__(self, parent, attrs, contents):
        super().__init__(parent, 'figure', attrs)

    def figcaption(self, *contents, **attrs):
        return FlowElement(self, 'figcaption', attrs, contents)

class Img(EmptyElement):
    def __init__(self, parent, attrs):
        super().__init__(parent, 'img', attrs)


# ---- Form and input tags ----

class Form(FlowElement):
    def __init__(self, parent, attrs, contents):
        super().__init__(parent, 'form', attrs, contents)

    def form(self, *contents, **attrs):
        raise NotImplementedError()

class Input(EmptyElement):
    def __init__(self, parent, attrs):
        super().__init__(parent, 'input', attrs)

class Button(PhraseElement):
    def __init__(self, parent, attrs, contents):
        super().__init__(parent, 'button', attrs, contents)

class Select(PhraseElement):
    def __init__(self, parent, attrs, contents):
        super().__init__(parent, 'select', attrs, contents)

    def optgroup(self, *contents, **attrs):
        return OptGroup(self, attrs, contents)

    def option(self, *contents, **attrs):
        return Option(self, attrs, contents)

class OptGroup(PhraseElement):
    def __init__(self, parent, attrs, contents):
        super().__init__(parent, 'optgroup', attrs, contents)

    def option(self, *contents, **attrs):
        return Option(self, attrs, contents)

class Option(PhraseElement):
    def __init__(self, parent, attrs, contents):
        super().__init__(parent, 'option', attrs, contents)

class TextArea(TextOnlyElement):
    def __init__(self, parent, attrs, contents):
        super().__init__(parent, 'textarea', attrs, contents)



class Data(PhraseElement):
    def __init__(self, parent, attrs, contents):
        super().__init__(parent, 'data', attrs, contents)

class Time(PhraseElement):
    def __init__(self, parent, attrs, contents):
        super().__init__(parent, 'time', attrs, contents)

class Details(PhraseElement):
    def __init__(self, parent, attrs, contents):
        super().__init__(parent, 'details', attrs, contents)

    def summary(self, *contents, **attrs):
        return Summary(self, attrs, contents)

class Summary(PhraseElement):
    def __init__(self, parent, attrs, contents):
        super().__init__(parent, 'summary', attrs, contents)

class Object(PhraseElement):
    def __init__(self, parent, attrs, contents):
        super().__init__(parent, 'object', attrs, contents)

    def param(self, **attrs):
        return Param(self, attrs)

class Param(EmptyElement):
    def __init__(self, parent, attrs):
        super().__init__(parent, 'param', attrs)



class TransparentElement(Element):
    pass

class Canvas(TransparentElement):
    def __init__(self, parent, attrs, contents):
        super().__init__(parent, 'canvas', attrs, contents)

# class Div(GeneralElement):
#     def __init__(self, parent, attrs, content):
#         super().__init__(parent, 'div', attrs, content)

# class Span(GeneralElement):
#     def __init__(self, parent, attrs):
#         super().__init__(parent, 'span', attrs, content)

# class P(GeneralElement):
#     def __init__(self, parent, attrs):
#         super().__init__(parent, 'p', attrs, content)


import sys

def main():
    print('---- Html-1a ----')
    with Page(FormattedStream(sys.stdout)) as page:
        page.doctype()
        with page.element('html') as html:
            with html.element('head') as head:
                head.element('title', {}, 'Sample Page')
                head.element('meta', {'charset':'utf-8'}, self_close=True)
            with html.element('body') as body:
                with body.element('h1') as title:
                    title.text('Sample Title')
                with body.element('div', {'class':'content'}) as content:
                    content.element('div', {'class':'page_id'}, 123)
                    content.text('Hello, Html.')
                    content.text('Foobar')
                    content.element('div', {'class':'box'}, 'Apple, Orange, Lemon')
                    
    print('---- Html-1b ----')
    with Page(FormattedStream(sys.stdout)) as page:
        page.doctype()
        with page.html() as html:
            with html.head() as head:
                head.title('Sample Page')
                head.meta(charset='utf-8')
            with html.body() as body:
                body.h1('Sample Title')
                with body.div_class('content') as content:
                    content.div_class('page_id', 123)
                    content.text('Hello, Html.')
                    content.text('Foobar')
                    content.div_class('box', 'Apple, Orange, Lemon')


    mat_data = [[x * y for x in range(1, 10)] for y in range(1, 10, 2)]


    print('---- Html-2a ----')
    with Page(FormattedStream(sys.stdout)) as page:
        page.doctype()
        with page.html() as html:
            with html.body() as body:
                with body.table() as table:
                    for i, row_data in enumerate(mat_data):
                        with table.tr('row_{}'.format(i)) as tr:
                            for j, val in enumerate(row_data):
                                tr.td_class('cell_{}_{}'.format(i, j), val)


    # print('---- Html-2b ----')
    # with Page(FormattedStream(sys.stdout)) as page:
    #     page.doctype()
    #     with page.html() as html:
    #         with html.body() as body:
    #             with body.table() as table:
    #                 table.cells(
    #                     mat_data,
    #                     lambda i, j, val: val,
    #                     lambda i, j, val: {'class': 'cell_{}_{}'.format(i, j)},
    #                     lambda i: {'class': 'row_{}'.format(i)},
    #                 )


if __name__ == "__main__":
    main()
