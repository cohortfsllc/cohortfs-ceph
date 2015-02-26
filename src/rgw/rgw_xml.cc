#include <string.h>

#include <iostream>
#include <map>

#include "include/types.h"

#include "rgw_common.h"
#include "rgw_xml.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

XMLObjIter::
XMLObjIter()
{
}

XMLObjIter::
~XMLObjIter()
{
}

void XMLObjIter::
set(const XMLObjIter::map_iter_t &_cur, const XMLObjIter::map_iter_t &_end)
{
  cur = _cur;
  end = _end;
}

XMLObj *XMLObjIter::
get_next()
{
  XMLObj *oid = NULL;
  if (cur != end) {
    oid = cur->second;
    ++cur;
  }
  return oid;
};

ostream& operator<<(ostream& out, XMLObj& oid) {
   out << oid.obj_type << ": " << oid.data;
   return out;
}

XMLObj::
~XMLObj()
{
}

bool XMLObj::
xml_start(XMLObj *parent, const char *el, const char **attr)
{
  this->parent = parent;
  obj_type = el;
  for (int i = 0; attr[i]; i += 2) {
    attr_map[attr[i]] = string(attr[i + 1]);
  }
  return true;
}

bool XMLObj::
xml_end(const char *el)
{
  return true;
}

void XMLObj::
xml_handle_data(const char *s, int len)
{
  data.append(s, len);
}

string& XMLObj::
XMLObj::get_data()
{
  return data;
}

XMLObj *XMLObj::
XMLObj::get_parent()
{
  return parent;
}

void XMLObj::
add_child(string el, XMLObj *oid)
{
  children.insert(pair<string, XMLObj *>(el, oid));
}

bool XMLObj::
get_attr(string name, string& attr)
{
  map<string, string>::iterator iter = attr_map.find(name);
  if (iter == attr_map.end())
    return false;
  attr = iter->second;
  return true;
}

XMLObjIter XMLObj::
find(string name)
{
  XMLObjIter iter;
  map<string, XMLObj *>::iterator first;
  map<string, XMLObj *>::iterator last;
  first = children.find(name);
  if (first != children.end()) {
    last = children.upper_bound(name);
  }else
    last = children.end();
  iter.set(first, last);
  return iter;
}

XMLObj *XMLObj::
find_first(string name)
{
  XMLObjIter iter;
  map<string, XMLObj *>::iterator first;
  first = children.find(name);
  if (first != children.end())
    return first->second;
  return NULL;
}
static void xml_start(void *data, const char *el, const char **attr) {
  RGWXMLParser *handler = static_cast<RGWXMLParser *>(data);

  if (!handler->xml_start(el, attr))
    handler->set_failure();
}

RGWXMLParser::
RGWXMLParser() : buf(NULL), buf_len(0), cur_obj(NULL), success(true)
{
  p = XML_ParserCreate(NULL);
}

RGWXMLParser::
~RGWXMLParser()
{
  XML_ParserFree(p);

  free(buf);
  vector<XMLObj *>::iterator iter;
  for (iter = objs.begin(); iter != objs.end(); ++iter) {
    XMLObj *oid = *iter;
    delete oid;
  }
}

bool RGWXMLParser::xml_start(const char *el, const char **attr) {
  XMLObj * oid = alloc_obj(el);
  if (!oid) {
    oid = new XMLObj();
  }
  if (!oid->xml_start(cur_obj, el, attr))
    return false;
  if (cur_obj) {
    cur_obj->add_child(el, oid);
  } else {
    children.insert(pair<string, XMLObj *>(el, oid));
  }
  cur_obj = oid;

  objs.push_back(oid);
  return true;
}

static void xml_end(void *data, const char *el) {
  RGWXMLParser *handler = static_cast<RGWXMLParser *>(data);

  if (!handler->xml_end(el))
    handler->set_failure();
}

bool RGWXMLParser::xml_end(const char *el) {
  XMLObj *parent_obj = cur_obj->get_parent();
  if (!cur_obj->xml_end(el))
    return false;
  cur_obj = parent_obj;
  return true;
}

static void handle_data(void *data, const char *s, int len)
{
  RGWXMLParser *handler = static_cast<RGWXMLParser *>(data);

  handler->handle_data(s, len);
}

void RGWXMLParser::handle_data(const char *s, int len)
{
  cur_obj->xml_handle_data(s, len);
}


bool RGWXMLParser::init()
{
  if (!p) {
    return false;
  }
  XML_SetElementHandler(p, ::xml_start, ::xml_end);
  XML_SetCharacterDataHandler(p, ::handle_data);
  XML_SetUserData(p, (void *)this);
  return true;
}

bool RGWXMLParser::parse(const char *_buf, int len, int done)
{
  int pos = buf_len;
  char *tmp_buf;
  tmp_buf = (char *)realloc(buf, buf_len + len);
  if (tmp_buf == NULL){
    free(buf);
    buf = NULL;
    return false;
  } else {
    buf = tmp_buf;
  }

  memcpy(&buf[buf_len], _buf, len);
  buf_len += len;

  success = true;
  if (!XML_Parse(p, &buf[pos], len, done)) {
    fprintf(stderr, "Parse error at line %d:\n%s\n",
	      (int)XML_GetCurrentLineNumber(p),
	      XML_ErrorString(XML_GetErrorCode(p)));
    success = false;
  }

  return success;
}
