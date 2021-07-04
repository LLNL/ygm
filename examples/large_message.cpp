// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <ygm/comm.hpp>

int main(int argc, char** argv) {

  ygm::comm world(&argc, &argv);

  std::string small_message = "Ground Control to Major Tom";

  std::string large_message = "Take your protein pills and put your helmet on. Commencing countdown, engines on. Check ignition and may God's love be with you.";

  // Define the function to execute in async
  auto howdy = [](auto pcomm, int from, const std::string& str) {
    std::cout << "Howdy, I'm rank " << pcomm->rank() << " (local rank " 
              << pcomm->local_rank() << ", remote rank " << pcomm->remote_rank()
              << "), and I received a message from rank " << from 
              << " that read: \"" << str << "\"" << std::endl;
  };

  //0 send small message to everyone and large message only to 1
  if (world.rank() == 0) {
    for (int dest = 0; dest < world.size(); ++dest) {
      world.async(dest, howdy, small_message);
    }

     world.async(1, howdy, large_message);
  }
  return 0;
}