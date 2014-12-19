#include <iostream>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include "include/msgr.h"
using namespace std;

int main() {
  string line;

  while(getline(cin, line)) {
    size_t left_bracket = line.find('[');
    size_t right_bracket = line.find(']');
    string time_stamp = line.substr(left_bracket + 1, right_bracket - 1); // to add: some way to add the dd-mm-yyyy to the timestamp

    size_t thread_beginning = line.find("thread", right_bracket);
    size_t thread_end = line.find(",", thread_beginning);
    string thread = line.substr(thread_beginning + 9, (thread_end) - (thread_beginning + 9));

    size_t entity_type_beginning = line.find("entity_type", thread_end);
    size_t entity_type_end = line.find(",", entity_type_beginning);
    string entity_type_int = line.substr(entity_type_beginning + 14, (entity_type_end) - (entity_type_beginning + 14));
    string entity_type = ceph_entity_type_name(atoi(entity_type_int.c_str()));

    size_t entity_name_beginning = line.find("entity_name", entity_type_end);
    size_t entity_name_end = line.find(",", entity_name_beginning);
    string entity_name = line.substr(entity_name_beginning + 15, (entity_name_end - 1) - (entity_name_beginning + 15));

    size_t prio_beginning = line.find("prio", right_bracket);
    size_t prio_end = line.find(",", prio_beginning);
    string prio = line.substr(prio_beginning + 7, prio_end - (prio_beginning + 7));

    size_t subsys_beginning = line.find("subsys", prio_end);
    size_t subsys_end = line.find(",", subsys_beginning);
    string subsys = line.substr(subsys_beginning + 9, subsys_end - (subsys_beginning + 9));

    size_t msg_beginning = line.find("msg", subsys_end);
    size_t msg_end = line.rfind("}");
    string msg = line.substr(msg_beginning + 7, (msg_end - 2) - (msg_beginning + 7));

    printf("%s %s %s.%s %2s %s\n", time_stamp.c_str(), thread.c_str(), entity_type.c_str(), entity_name.c_str(), prio.c_str(), msg.c_str());

  }

  return 0;
}
