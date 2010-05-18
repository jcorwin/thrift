/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <sstream>
#include <string>
#include <fstream>
#include <iostream>
#include <vector>
#include <cctype>

#include <sys/stat.h>
#include <stdexcept>

#include "platform.h"
#include "t_oop_generator.h"
using namespace std;


/**
 * Scala code generator.
 *
 */
class t_scala_generator : public t_oop_generator {
 public:
  t_scala_generator(
      t_program* program,
      const std::map<std::string, std::string>& parsed_options,
      const std::string& option_string)
    : t_oop_generator(program)
  {
    out_dir_base_ = "gen-scala";
  }

  /**
   * Init and close methods
   */

  void init_generator();
  void close_generator();
  void generate_consts(std::vector<t_const*> consts);

  /**
   * Program-level generation functions
   */

  void generate_typedef (t_typedef*  ttypedef);
  void generate_enum    (t_enum*     tenum);
  void generate_struct  (t_struct*   tstruct);
  void generate_union   (t_struct*   tunion);
  void generate_xception(t_struct*   txception);
  void generate_service (t_service*  tservice);

  /**
   * Helper rendering functions
   */

  void generate_scala_struct(t_struct* tstruct, bool is_exception);
  void generate_scala_struct_definition(ofstream &out, t_struct* tstruct, bool is_exception);
  std::string get_scala_type_string(t_type* type);

  void generate_service_interface (t_service* tservice);
  void generate_service_helpers   (t_service* tservice);
  void generate_service_client    (t_service* tservice);
  void generate_service_server    (t_service* tservice);

  std::string scala_package();
  std::string scala_type_imports();
  std::string scala_thrift_imports();
  std::string type_name(t_type* ttype, bool in_container=false, bool in_init=false, bool skip_generic=false);
  std::string base_type_name(t_base_type* tbase, bool in_container=false);
  std::string constant_name(std::string name);
  std::string get_cap_name(std::string name);

  void print_const_value(std::ofstream& out, std::string name, t_type* type, t_const_value* value, bool in_static, bool defval=false);

  std::string render_const_value(std::ofstream& out, std::string name, t_type* type, t_const_value* value);

  std::string get_enum_class_name(t_type* type);
  void generate_scala_doc                 (std::ofstream& out,
                                           t_field*    field);

  void generate_scala_doc                 (std::ofstream& out,
                                           t_doc*      tdoc);

  void generate_scala_doc                 (std::ofstream& out,
                                           t_function* tdoc);

  void generate_scala_docstring_comment   (std::ofstream &out,
                                           string contents);

 private:

  /**
   * File streams
   */

  std::string package_name_;
  std::ofstream f_service_;
  std::string package_dir_;
};


/**
 * Prepares for file generation by opening up the necessary file output
 * streams.
 *
 * @param tprogram The program to generate
 */
void t_scala_generator::init_generator() {
  // Make output directory
  MKDIR(get_out_dir().c_str());
  package_name_ = program_->get_namespace("scala");

  string dir = package_name_;
  string subdir = get_out_dir();
  string::size_type loc;
  while ((loc = dir.find(".")) != string::npos) {
    subdir = subdir + "/" + dir.substr(0, loc);
    MKDIR(subdir.c_str());
    dir = dir.substr(loc+1);
  }
  if (dir.size() > 0) {
    subdir = subdir + "/" + dir;
    MKDIR(subdir.c_str());
  }

  package_dir_ = subdir;
}

/**
 * Packages the generated file
 *
 * @return String of the package, i.e. "package org.apache.thriftdemo;"
 */
string t_scala_generator::scala_package() {
  if (!package_name_.empty()) {
    return string("package ") + package_name_ + "\n\n";
  }
  return "";
}

/**
 * Prints standard scala imports
 *
 * @return List of imports for Scala types that are used in here
 */
string t_scala_generator::scala_type_imports() {
  return
    string() +
      "import org.slf4j.{Logger,LoggerFactory}\n";
}

/**
 * Prints standard scala imports
 *
 * @return List of imports necessary for thrift
 */
string t_scala_generator::scala_thrift_imports() {
  return
    string() +
    "import org.apache.thrift._\n" +
    "import org.apache.thrift.meta_data._\n" +
    "import org.apache.thrift.protocol._\n\n";
}

/**
 * Nothing in Scala
 */
void t_scala_generator::close_generator() {}

/**
 * Generates a typedef. 
 *
 * @param ttypedef The type definition
 */
void t_scala_generator::generate_typedef(t_typedef* ttypedef) {
    // FIXME: implement
}

/**
 * Enums are a class with a set of static constants.
 *
 * @param tenum The enumeration
 */
void t_scala_generator::generate_enum(t_enum* tenum) {
  // Make output file
  string f_enum_name = package_dir_+"/"+(tenum->get_name())+".scala";
  ofstream f_enum;
  f_enum.open(f_enum_name.c_str());

  // Comment and package it
  f_enum <<
    autogen_comment() <<
    scala_package() << endl;

  generate_scala_doc(f_enum, tenum);
  indent(f_enum) <<
    "object " << tenum->get_name() << " extends Enumeration";
  scope_up(f_enum);

  vector<t_enum_value*> constants = tenum->get_constants();
  vector<t_enum_value*>::iterator c_iter;

  int value = -1;
  for (c_iter = constants.begin(); c_iter != constants.end(); ++c_iter) {
    if ((*c_iter)->has_value()) {
      value = (*c_iter)->get_value();
    } else {
      ++value;
    }

    generate_scala_doc(f_enum, *c_iter);
    indent(f_enum) << "val " << (*c_iter)->get_name() << " = Value(" << value << ")" << endl;
  }
  scope_down(f_enum);

  f_enum.close();
}

/**
 * Generates a class that holds all the constants.
 */
void t_scala_generator::generate_consts(std::vector<t_const*> consts) {
  if (consts.empty()) {
    return;
  }

  string f_consts_name = package_dir_+"/Constants.scala";
  ofstream f_consts;
  f_consts.open(f_consts_name.c_str());

  // Print header
  f_consts <<
      autogen_comment() <<
      scala_package(); 

  f_consts <<
    "object Constants {" << endl <<
    endl;
  indent_up();
  vector<t_const*>::iterator c_iter;
  for (c_iter = consts.begin(); c_iter != consts.end(); ++c_iter) {
    print_const_value(f_consts,
                      (*c_iter)->get_name(),
                      (*c_iter)->get_type(),
                      (*c_iter)->get_value(),
                      false);
  }
  indent_down();
  indent(f_consts) <<
    "}" << endl;
  f_consts.close();
}


/**
 * Prints the value of a constant with the given type. Note that type checking
 * is NOT performed in this function as it is always run beforehand using the
 * validate_types method in main.cc
 */
void t_scala_generator::print_const_value(std::ofstream& out, string name, t_type* type, t_const_value* value, bool in_static, bool defval) {
  type = get_true_type(type);

  indent(out);
  if (!defval) {
    out <<
        (in_static ? "" : "val ") << name << " : " <<
        type_name(type);
  }
  if (type->is_base_type()) {
    string v2 = render_const_value(out, name, type, value);
    out << " = " << v2 << endl << endl;
  } else if (type->is_enum()) {
    out << " = " << value->get_integer() << endl << endl;
  } else if (type->is_struct() || type->is_xception()) {
      /*
    const vector<t_field*>& fields = ((t_struct*)type)->get_members();
    vector<t_field*>::const_iterator f_iter;
    const map<t_const_value*, t_const_value*>& val = value->get_map();
    map<t_const_value*, t_const_value*>::const_iterator v_iter;
    out << " = new " << type_name(type, false, true) << "();" << endl;
    if (!in_static) {
      indent(out) << "static {" << endl;
      indent_up();
    }
    for (v_iter = val.begin(); v_iter != val.end(); ++v_iter) {
      t_type* field_type = NULL;
      for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
        if ((*f_iter)->get_name() == v_iter->first->get_string()) {
          field_type = (*f_iter)->get_type();
        }
      }
      if (field_type == NULL) {
        throw "type error: " + type->get_name() + " has no field " + v_iter->first->get_string();
      }
      string val = render_const_value(out, name, field_type, v_iter->second);
      indent(out) << name << ".";
      std::string cap_name = get_cap_name(v_iter->first->get_string());
      out << "set" << cap_name << "(" << val << ");" << endl;
    }
    if (!in_static) {
      indent_down();
      indent(out) << "}" << endl;
    }
    out << endl;
      */
  } else if (type->is_map()) {
      /*
    out << name << " = new " << type_name(type, false, true) << "();" << endl;
    if (!in_static) {
      indent(out) << "static {" << endl;
      indent_up();
    }
    t_type* ktype = ((t_map*)type)->get_key_type();
    t_type* vtype = ((t_map*)type)->get_val_type();
    const map<t_const_value*, t_const_value*>& val = value->get_map();
    map<t_const_value*, t_const_value*>::const_iterator v_iter;
    for (v_iter = val.begin(); v_iter != val.end(); ++v_iter) {
      string key = render_const_value(out, name, ktype, v_iter->first);
      string val = render_const_value(out, name, vtype, v_iter->second);
      indent(out) << name << ".put(" << key << ", " << val << ");" << endl;
    }
    if (!in_static) {
      indent_down();
      indent(out) << "}" << endl;
    }
    out << endl;
      */
  } else if (type->is_list() || type->is_set()) {
      /*
    out << name << " = new " << type_name(type, false, true) << "();" << endl;
    if (!in_static) {
      indent(out) << "static {" << endl;
      indent_up();
    }
    t_type* etype;
    if (type->is_list()) {
      etype = ((t_list*)type)->get_elem_type();
    } else {
      etype = ((t_set*)type)->get_elem_type();
    }
    const vector<t_const_value*>& val = value->get_list();
    vector<t_const_value*>::const_iterator v_iter;
    for (v_iter = val.begin(); v_iter != val.end(); ++v_iter) {
      string val = render_const_value(out, name, etype, *v_iter);
      indent(out) << name << ".add(" << val << ");" << endl;
    }
    if (!in_static) {
      indent_down();
      indent(out) << "}" << endl;
    }
    out << endl;
      */
  } else {
    throw "compiler error: no const of type " + type->get_name();
  }
}

string t_scala_generator::render_const_value(ofstream& out, string name, t_type* type, t_const_value* value) {
  type = get_true_type(type);
  std::ostringstream render;

  if (type->is_base_type()) {
    t_base_type::t_base tbase = ((t_base_type*)type)->get_base();
    switch (tbase) {
    case t_base_type::TYPE_STRING:
      render << '"' << get_escaped_string(value) << '"';
      break;
    case t_base_type::TYPE_BOOL:
      render << ((value->get_integer() > 0) ? "true" : "false");
      break;
    case t_base_type::TYPE_BYTE:
      render << "(byte)" << value->get_integer();
      break;
    case t_base_type::TYPE_I16:
      render << "(short)" << value->get_integer();
      break;
    case t_base_type::TYPE_I32:
      render << value->get_integer();
      break;
    case t_base_type::TYPE_I64:
      render << value->get_integer() << "L";
      break;
    case t_base_type::TYPE_DOUBLE:
      if (value->get_type() == t_const_value::CV_INTEGER) {
        render << "(double)" << value->get_integer();
      } else {
        render << value->get_double();
      }
      break;
    default:
      throw "compiler error: no const of base type " + t_base_type::t_base_name(tbase);
    }
  } else if (type->is_enum()) {
    render << value->get_integer();
  } else {
    string t = tmp("tmp");
    print_const_value(out, t, type, value, true);
    render << t;
  }

  return render.str();
}

/**
 * Generates a struct definition for a thrift data type. This will be a TBase 
 * implementor.
 *
 * @param tstruct The struct definition
 */
void t_scala_generator::generate_struct(t_struct* tstruct) {
    generate_scala_struct(tstruct, false);
}

/**
 * Exceptions are structs, but they inherit from Exception
 *
 * @param tstruct The struct definition
 */
void t_scala_generator::generate_xception(t_struct* txception) {
  generate_scala_struct(txception, true);
}


/**
 * Scala struct definition.
 *
 * @param tstruct The struct definition
 */
void t_scala_generator::generate_scala_struct(t_struct* tstruct,
                                            bool is_exception) {
  // Make output file
  string f_struct_name = package_dir_+"/"+(tstruct->get_name())+".scala";
  ofstream f_struct;
  f_struct.open(f_struct_name.c_str());

  f_struct <<
    autogen_comment() <<
    scala_package() <<
    scala_type_imports() <<
    scala_thrift_imports();

  generate_scala_struct_definition(f_struct,
                                   tstruct,
                                   is_exception);
  f_struct.close();
}

/**
 * Scala struct definition. This has various parameters, as it could be
 * generated standalone or inside another class as a helper. If it
 * is a helper than it is a static class.
 *
 * @param tstruct      The struct definition
 * @param is_exception Is this an exception?
 * @param in_class     If inside a class, needs to be static class
 * @param is_result    If this is a result it needs a different writer
 */
void t_scala_generator::generate_scala_struct_definition(ofstream &out,
                                                         t_struct* tstruct,
                                                         bool is_exception) {
  generate_scala_doc(out, tstruct);

  indent(out) << "case class " << tstruct->get_name() << "(";

  // Members are var parameters for -scala
  const vector<t_field*>& members = tstruct->get_members();
  vector<t_field*>::const_iterator m_iter;

  bool first = true;
  for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
      if (!first) {
          out << ", ";
      }
      first = false;
      generate_scala_doc(out, *m_iter);
      out << "var " << (*m_iter)->get_name() << " : " << type_name((*m_iter)->get_type());
  }
  out << ")";

  out << " extends TBase[" << tstruct->get_name() << "._FIELDS]";
  if (is_exception) {
    out << " with Exception";
  }

  scope_up(out);

  scope_down(out);
}

/** 
 * Returns a string with the scala representation of the given thrift type
 * (e.g. for the type struct it returns "TType.STRUCT")
 */
std::string t_scala_generator::get_scala_type_string(t_type* type) {
  if (type->is_list()){
    return "TType.LIST";
  } else if (type->is_map()) {
    return "TType.MAP";
  } else if (type->is_set()) {
    return "TType.SET";
  } else if (type->is_struct() || type->is_xception()) {
    return "TType.STRUCT";
  } else if (type->is_enum()) {
    return "TType.ENUM";
  } else if (type->is_typedef()) {
    return get_scala_type_string(((t_typedef*)type)->get_type());
  } else if (type->is_base_type()) {
    switch (((t_base_type*)type)->get_base()) {
      case t_base_type::TYPE_VOID   : return      "TType.VOID"; break;
      case t_base_type::TYPE_STRING : return    "TType.STRING"; break;
      case t_base_type::TYPE_BOOL   : return      "TType.BOOL"; break;
      case t_base_type::TYPE_BYTE   : return      "TType.BYTE"; break;
      case t_base_type::TYPE_I16    : return       "TType.I16"; break;
      case t_base_type::TYPE_I32    : return       "TType.I32"; break;
      case t_base_type::TYPE_I64    : return       "TType.I64"; break;
      case t_base_type::TYPE_DOUBLE : return    "TType.DOUBLE"; break;
      default : throw std::runtime_error("Unknown thrift type \"" + type->get_name() + "\" passed to t_scala_generator::get_scala_type_string!"); break; // This should never happen!
    }
  } else {
    throw std::runtime_error("Unknown thrift type \"" + type->get_name() + "\" passed to t_scala_generator::get_scala_type_string!"); // This should never happen!
  }
}

/**
 * Generates a thrift service.
 *
 * @param tservice The service definition
 */
void t_scala_generator::generate_service(t_service* tservice) {
  // Make output file
  string f_service_name = package_dir_+"/"+service_name_+".scala";
  f_service_.open(f_service_name.c_str());

  f_service_ <<
    autogen_comment() <<
    scala_package() <<
    scala_type_imports() <<
    scala_thrift_imports();

  f_service_ <<
    "class " << service_name_ << " {" << endl <<
    endl;
  indent_up();

  // Generate the three main parts of the service
  generate_service_interface(tservice);
  generate_service_client(tservice);
  generate_service_server(tservice);
  generate_service_helpers(tservice);

  indent_down();
  f_service_ <<
    "}" << endl;
  f_service_.close();
}

/**
 * Generates a service interface definition.
 *
 * @param tservice The service to generate a header definition for
 */
void t_scala_generator::generate_service_interface(t_service* tservice) {
}

/**
 * Generates structs for all the service args and return types
 *
 * @param tservice The service
 */
void t_scala_generator::generate_service_helpers(t_service* tservice) {
}

/**
 * Generates a service client definition.
 *
 * @param tservice The service to generate a server for.
 */
void t_scala_generator::generate_service_client(t_service* tservice) {
}

/**
 * Generates a service server definition.
 *
 * @param tservice The service to generate a server for.
 */
void t_scala_generator::generate_service_server(t_service* tservice) {

}

/**
 * Returns a Scala type name
 *
 * @param ttype The type
 * @param container Is the type going inside a container?
 * @return Scala type name, i.e. HashMap<Key,Value>
 */
string t_scala_generator::type_name(t_type* ttype, bool in_container, bool in_init, bool skip_generic) {
  // In Scala typedefs are just resolved to their real type
  ttype = get_true_type(ttype);
  string prefix;

  if (ttype->is_base_type()) {
    return base_type_name((t_base_type*)ttype, in_container);
  } else if (ttype->is_map()) {
    t_map* tmap = (t_map*) ttype;
    return string() + "Map[" + 
        type_name(tmap->get_key_type(), true) +
        "," + type_name(tmap->get_val_type(), true) +
        "]";
  } else if (ttype->is_set()) {
    t_set* tset = (t_set*) ttype;
    return string() + "Set[" + type_name(tset->get_elem_type(), true) + "]";
  } else if (ttype->is_list()) {
    t_list* tlist = (t_list*) ttype;
    return string() + "List[" + type_name(tlist->get_elem_type(), true) + "]";
  }

  // Check for namespacing
  t_program* program = ttype->get_program();
  if (program != NULL && program != program_) {
    string package = program->get_namespace("scala");
    if (!package.empty()) {
      return package + "." + ttype->get_name();
    }
  }

  return ttype->get_name();
}

/**
 * Returns the Scala type that corresponds to the thrift type.
 *
 * @param tbase The base type
 * @param container Is it going in a Scala container?
 */
string t_scala_generator::base_type_name(t_base_type* type,
                                        bool in_container) {
  t_base_type::t_base tbase = type->get_base();

  switch (tbase) {
  case t_base_type::TYPE_VOID:
    return "void";
  case t_base_type::TYPE_STRING:
    if (type->is_binary()) {
      return "Array[Byte]";
    } else {
      return "String";
    }
  case t_base_type::TYPE_BOOL:
      return "Boolean";
  case t_base_type::TYPE_BYTE:
      return "Byte";
  case t_base_type::TYPE_I16:
      return "Short";
  case t_base_type::TYPE_I32:
      return "Int";
  case t_base_type::TYPE_I64:
      return "Long";
  case t_base_type::TYPE_DOUBLE:
      return "Double";
  default:
    throw "compiler error: no C++ name for base type " + t_base_type::t_base_name(tbase);
  }
}

/**
 * Applies the correct style to a string based on the value of nocamel_style_
 */
std::string t_scala_generator::get_cap_name(std::string name){
    name[0] = toupper(name[0]);
    return name;
}

string t_scala_generator::constant_name(string name) {
  string constant_name;

  bool is_first = true;
  bool was_previous_char_upper = false;
  for (string::iterator iter = name.begin(); iter != name.end(); ++iter) {
    string::value_type character = (*iter);

    bool is_upper = isupper(character);

    if (is_upper && !is_first && !was_previous_char_upper) {
      constant_name += '_';
    }
    constant_name += toupper(character);

    is_first = false;
    was_previous_char_upper = is_upper;
  }

  return constant_name;
}

void t_scala_generator::generate_scala_docstring_comment(ofstream &out, string contents) {
  generate_docstring_comment(out,
    "/**\n",
    " * ", contents,
    " */\n");
}

std::string t_scala_generator::get_enum_class_name(t_type* type) {
  string package = "";
  t_program* program = type->get_program();
  if (program != NULL && program != program_) {
    package = program->get_namespace("java") + ".";
  }
  return package + type->get_name();
}

void t_scala_generator::generate_scala_doc(ofstream &out,
                                         t_field* field) {
  if (field->get_type()->is_enum()) {
    string combined_message = field->get_doc() + "\n@see " + get_enum_class_name(field->get_type());
    generate_scala_docstring_comment(out, combined_message);
  } else {
    generate_scala_doc(out, (t_doc*)field);
  }
}

/**
 * Emits a ScalaDoc comment if the provided object has a doc in Thrift
 */
void t_scala_generator::generate_scala_doc(ofstream &out,
                                         t_doc* tdoc) {
  if (tdoc->has_doc()) {
    generate_scala_docstring_comment(out, tdoc->get_doc());
  }
}

/**
 * Emits a ScalaDoc comment if the provided function object has a doc in Thrift
 */
void t_scala_generator::generate_scala_doc(ofstream &out,
                                         t_function* tfunction) {
  if (tfunction->has_doc()) {
    stringstream ss;
    ss << tfunction->get_doc();
    const vector<t_field*>& fields = tfunction->get_arglist()->get_members();
    vector<t_field*>::const_iterator p_iter;
    for (p_iter = fields.begin(); p_iter != fields.end(); ++p_iter) {
      t_field* p = *p_iter;
      ss << "\n@param " << p->get_name();
      if (p->has_doc()) {
        ss << " " << p->get_doc();
      }
    }
    generate_docstring_comment(out,
      "/**\n",
      " * ", ss.str(),
      " */\n");
  }
}

THRIFT_REGISTER_GENERATOR(scala, "Scala", "");
