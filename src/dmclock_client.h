// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2017 Red Hat Inc.
 *
 * Author: J. Eric Ivancich <ivancich@redhat.com>
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version
 * 2.1, as published by the Free Software Foundation.  See file
 * COPYING.
 */


#pragma once

#include <map>
#include <deque>
#include <list>
#include <chrono>
#include <thread>
#include <mutex>
#include <condition_variable>

#include "run_every.h"
#include "dmclock_util.h"
#include "dmclock_recs.h"


namespace crimson {
  namespace dmclock {

    constexpr Counter calc_interval = 2;
    constexpr Counter history_len = 30;

    // OrigTracker is a best-effort implementation of the the original
    // dmClock calculations of delta and rho. It adheres to an
    // interface, implemented via a template type, that allows it to
    // be replaced with an alternative. The interface consists of the
    // static create, prepare_req, resp_update, and get_last_delta
    // functions.
    class OrigTracker {
      Counter   delta_prev_req;
      Counter   rho_prev_req;
      Counter   beta_prev_req;  // beta for bandwidth
      uint32_t  my_delta;
      uint32_t  my_rho;
      Counter   my_beta;

    public:

      OrigTracker(Counter global_delta,
		  Counter global_rho,
		  Counter global_beta) :
	delta_prev_req(global_delta),
	rho_prev_req(global_rho),
	beta_prev_req(global_beta),
	my_delta(0),
	my_rho(0),
	my_beta(0)
      { /* empty */ }

      static inline OrigTracker create(Counter the_delta, Counter the_rho, Counter the_beta) {
	return OrigTracker(the_delta, the_rho, the_beta);
      }

      inline ReqParams prepare_req(Counter& the_delta, Counter& the_rho, Counter& the_beta) {
	Counter delta_out = the_delta - delta_prev_req - my_delta;
	Counter rho_out = the_rho - rho_prev_req - my_rho;
	Counter beta_out = the_beta - beta_prev_req - my_beta;
	delta_prev_req = the_delta;
	rho_prev_req = the_rho;
	beta_prev_req = the_beta;
	my_delta = 0;
	my_rho = 0;
	my_beta = 0;
	return ReqParams(uint32_t(delta_out), uint32_t(rho_out), Counter(beta_out));
      }

      inline void resp_update(PhaseType phase, Cost cost, Counter byte) {
	my_delta += cost;
	if (phase == PhaseType::reservation) {
	  my_rho += cost;
	}
	my_beta += byte;
      }

      inline Counter get_last_delta() const {
	return delta_prev_req;
      }
    }; // struct OrigTracker


    // BorrowingTracker always returns a positive delta and rho. If
    // not enough responses have come in to allow that, we will borrow
    // a future response and repay it later.
    class BorrowingTracker {
      Counter delta_prev_req;
      Counter rho_prev_req;
      Counter beta_prev_req;
      Counter delta_borrow;
      Counter rho_borrow;
      Counter beta_borrow;

    public:

      BorrowingTracker(Counter global_delta, Counter global_rho, Counter global_beta) :
	delta_prev_req(global_delta),
	rho_prev_req(global_rho),
	beta_prev_req(global_beta),
	delta_borrow(0),
	rho_borrow(0),
	beta_borrow(0)
      { /* empty */ }

      static inline BorrowingTracker create(Counter the_delta,
					    Counter the_rho,
					    Counter the_beta) {
	return BorrowingTracker(the_delta, the_rho, the_beta);
      }

      inline Counter calc_with_borrow(const Counter& global,
				      const Counter& previous,
				      Counter& borrow) {
	Counter result = global - previous;
	if (0 == result) {
	  // if no replies have come in, borrow one from the future
	  ++borrow;
	  return 1;
	} else if (result > borrow) {
	  // if we can give back all of what we borrowed, do so
	  result -= borrow;
	  borrow = 0;
	  return result;
	} else {
	  // can only return part of what was borrowed in order to
	  // return positive
	  borrow = borrow - result + 1;
	  return 1;
	}
      }

      inline ReqParams prepare_req(Counter& the_delta, Counter& the_rho, Counter& the_beta) {
	Counter delta_out =
	  calc_with_borrow(the_delta, delta_prev_req, delta_borrow);
	Counter rho_out =
	  calc_with_borrow(the_rho, rho_prev_req, rho_borrow);
	Counter beta_out =
	  calc_with_borrow(the_beta, rho_prev_req, beta_borrow);
	delta_prev_req = the_delta;
	rho_prev_req = the_rho;
	beta_prev_req = the_beta;
	return ReqParams(uint32_t(delta_out), uint32_t(rho_out), Counter(beta_out));
      }

      inline void resp_update(PhaseType phase, Counter cost, Counter byte) {
      }

      inline Counter get_last_delta() const {
	return delta_prev_req;
      }
    }; // struct BorrowingTracker


    /*
     * S is server identifier type
     *
     * T is the server info class that adheres to ServerTrackerIfc
     * interface
     */
    template<typename S, typename T = OrigTracker>
    class ServiceTracker {
      // we don't want to include gtest.h just for FRIEND_TEST
      friend class dmclock_client_server_erase_Test;

      using TimePoint = decltype(std::chrono::steady_clock::now());
      using Duration = std::chrono::milliseconds;
      using MarkPoint = std::pair<TimePoint,Counter>;

      Counter                 delta_counter; // # reqs completed
      Counter                 rho_counter;   // # reqs completed via reservation
      Counter                 beta_bytes;    // # req's bytes completed
      Counter                 delta_counter_prev;
      Counter                 rho_counter_prev;
      Counter                 beta_bytes_prev;
      std::list<double>       history_rates[3];  // delta, rho, beta

      std::map<S,T>           server_map;
      mutable std::mutex      data_mtx;      // protects Counters and map

      using DataGuard = std::lock_guard<decltype(data_mtx)>;

      // clean config

      std::deque<MarkPoint>     clean_mark_points;
      Duration                  clean_age;     // age at which server tracker cleaned

      // NB: All threads declared at end, so they're destructed firs!

      std::unique_ptr<RunEvery> cleaning_job;
      std::unique_ptr<RunEvery> svc_tick_job;


    public:

      // we have to start the counters at 1, as 0 is used in the
      // cleaning process
      template<typename Rep, typename Per>
      ServiceTracker(std::chrono::duration<Rep,Per> _clean_every,
		     std::chrono::duration<Rep,Per> _clean_age) :
	delta_counter(1),
	rho_counter(1),
	beta_bytes(1),
	delta_counter_prev(0),
	rho_counter_prev(0),
	beta_bytes_prev(0),
	clean_age(std::chrono::duration_cast<Duration>(_clean_age))
      {
	cleaning_job =
	  std::unique_ptr<RunEvery>(
	    new RunEvery(_clean_every,
			 std::bind(&ServiceTracker::do_clean, this)));
        svc_tick_job =
          std::unique_ptr<RunEvery>(
            new RunEvery(std::chrono::seconds(calc_interval),
                         std::bind(&ServiceTracker::calc_svc_rate, this))
          );

      }


      // the reason we're overloading the constructor rather than
      // using default values for the arguments is so that callers
      // have to either use all defaults or specify all timings; with
      // default arguments they could specify some without others
      ServiceTracker() :
	ServiceTracker(std::chrono::minutes(5), std::chrono::minutes(10))
      {
	// empty
      }


      /*
       * Incorporates the response data received into the counters.
       */
      void track_resp(const S& server_id,
		      const PhaseType& phase,
		      Counter request_cost = 1u,
		      Counter request_byte = 0) {
	DataGuard g(data_mtx);

	auto it = server_map.find(server_id);
	if (server_map.end() == it) {
	  // this code can only run if a request did not precede the
	  // response or if the record was cleaned up b/w when
	  // the request was made and now
	  auto i = server_map.emplace(server_id,
				      T::create(delta_counter, rho_counter, beta_bytes));
	  it = i.first;
	}
	it->second.resp_update(phase, request_cost, request_byte);

        delta_counter += request_cost;
        if (PhaseType::reservation == phase) {
          rho_counter += request_cost;
        }
        beta_bytes += request_byte;
      }

      /*
       * Returns the ReqParams for the given server.
       */
      ReqParams get_req_params(const S& server) {
	DataGuard g(data_mtx);
	auto it = server_map.find(server);
	if (server_map.end() == it) {
	  server_map.emplace(server,
			     T::create(delta_counter, rho_counter, beta_bytes));
	  return ReqParams(1, 1, 1);
	} else {
	  return it->second.prepare_req(delta_counter, rho_counter, beta_bytes);
	}
      }

      void get_average_rates(double *delta, double *rho, double *beta) {
        double avg_rate = 0;
        if (delta != nullptr) {
          for (auto i : history_rates[0]) {
            avg_rate += i;
          }
          *delta = avg_rate / history_rates[0].size();
        }

        avg_rate = 0;
        if (rho != nullptr) {
          for (auto i : history_rates[1]) {
            avg_rate += i;
          }
          *rho = avg_rate / history_rates[1].size();
        }

        avg_rate = 0;
        if (beta != nullptr) {
          for (auto i : history_rates[2]) {
            avg_rate += i;
          }
          *beta = avg_rate / history_rates[2].size();
        }
      }

    private:

      /*
       * This is being called regularly by RunEvery. Every time it's
       * called it notes the time and delta counter (mark point) in a
       * deque. It also looks at the deque to find the most recent
       * mark point that is older than clean_age. It then walks the
       * map and delete all server entries that were last used before
       * that mark point.
       */
      void do_clean() {
	TimePoint now = std::chrono::steady_clock::now();
	DataGuard g(data_mtx);
	clean_mark_points.emplace_back(MarkPoint(now, delta_counter));

	Counter earliest = 0;
	auto point = clean_mark_points.front();
	while (point.first <= now - clean_age) {
	  earliest = point.second;
	  clean_mark_points.pop_front();
	  point = clean_mark_points.front();
	}

	if (earliest > 0) {
	  for (auto i = server_map.begin();
	       i != server_map.end();
	       /* empty */) {
	    auto i2 = i++;
	    if (i2->second.get_last_delta() <= earliest) {
	      server_map.erase(i2);
	    }
	  }
	}
      } // do_clean

      void calc_svc_rate() {
        DataGuard g(data_mtx);
        assert(calc_interval > 0);

        auto delta_rate = (delta_counter - delta_counter_prev) / (double)calc_interval;
        history_rates[0].push_back(delta_rate);
        delta_counter_prev = delta_counter;

        auto rho_rate = (rho_counter - rho_counter_prev) / (double)calc_interval;
        history_rates[1].push_back(rho_rate);
        rho_counter_prev = rho_counter;

        auto beta_rate = (beta_bytes - beta_bytes_prev) / (double)calc_interval;
        history_rates[2].push_back(beta_rate);
        beta_bytes_prev = beta_bytes;

        for (auto i = 0; i < 3; i++) {
          while(history_rates[i].size() > history_len) {
            history_rates[i].pop_front();
          }
        }
      }
    }; // class ServiceTracker
  }
}
