#include <iostream>
#include <string>
#include <stdio.h>
using namespace std;

int main() {
  string line;

  while(getline(cin, line)) {
    size_t left_bracket = line.find('[');
    size_t right_bracket = line.find(']');
    string time_stamp = line.substr(left_bracket + 1, right_bracket - 1); // to add: some way to add the dd-mm-yyyy to the timestamp

    size_t prio_beginning = line.find("prio", right_bracket);
    size_t prio_end = line.find(",", prio_beginning);
    string prio = line.substr(prio_beginning + 7, prio_end - (prio_beginning + 7));

    size_t subsys_beginning = line.find("subsys", prio_end);
    size_t subsys_end = line.find(",", subsys_beginning);
    string subsys = line.substr(subsys_beginning + 9, subsys_end - (subsys_beginning + 9));

    size_t msg_beginning = line.find("msg", subsys_end);
    size_t msg_end = line.rfind("}");
    string msg = line.substr(msg_beginning + 7, (msg_end - 2) - (msg_beginning + 7));

    printf("%s %2s %s\n", time_stamp.c_str(), prio.c_str(), msg.c_str());

  }

  return 0;
}
