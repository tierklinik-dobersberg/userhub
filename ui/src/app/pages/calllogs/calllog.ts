import { Component, OnDestroy, OnInit, TrackByFunction } from "@angular/core";
import { sum } from "ng-zorro-antd/core/util";
import { BehaviorSubject, combineLatest, forkJoin, interval, Observable, of, Subject, Subscription } from "rxjs";
import { catchError, mergeMap, startWith } from "rxjs/operators";
import { CallLog, CalllogAPI, ConfigAPI, IdentityAPI, ProfileWithAvatar } from "src/app/api";
import { Customer, CustomerAPI } from "src/app/api/customer.api";
import { LayoutService } from "src/app/layout.service";
import { HeaderTitleService } from "src/app/shared/header-title";

interface LocalCallLog extends CallLog {
  localDate: string;
  localTime: string;
  customer?: Customer;
  agentUser?: Partial<ProfileWithAvatar>;
  transferToUser?: Partial<ProfileWithAvatar>;
}

@Component({
  templateUrl: './calllog.html',
  styleUrls: ['./calllog.scss'],
})
export class CallLogComponent implements OnInit, OnDestroy {
  private subscriptions = Subscription.EMPTY;
  private _selectedDate = new BehaviorSubject<Date>(new Date());
  private ready = new Subject<void>();

  date: Date;

  totalCallTime: number = 0;
  logs: LocalCallLog[] = [];

  private usersByExtensions: Map<string, ProfileWithAvatar> = new Map();
  private knownExtensioins: Map<string, Partial<ProfileWithAvatar>> = new Map();

  trackLog: TrackByFunction<CallLog> = (i: number, l: LocalCallLog) => {
    if (!!l) {
      return l._id!;
    }

    return i;
  };

  constructor(
    private header: HeaderTitleService,
    private calllogapi: CalllogAPI,
    private customerapi: CustomerAPI,
    private configapi: ConfigAPI,
    private identityapi: IdentityAPI,
    public layout: LayoutService,
  ) { }

  onChange(date: Date) {
    this._selectedDate.next(date);
  }

  ngOnInit() {
    this.header.set('Anruf Journal');
    this.date = new Date();
    this.subscriptions = new Subscription();

    const userSub = combineLatest([
      this.identityapi.listUsers(),
      this.configapi.change
    ]).subscribe(([users, config]) => {
      this.usersByExtensions = new Map();
      (config.UserPhoneExtensionProperties || []).forEach(propName => {
        users.forEach(user => {
          let ext = (user.properties || {})[propName];
          if (!!ext) {
            this.usersByExtensions.set(ext, user)
          }
        })
      });

      this.knownExtensioins = new Map();
      (config.KnownPhoneExtensions || []).forEach(known => {
        this.knownExtensioins.set(known.ExtensionNumber, { fullname: known.DisplayName })
      })

      this.ready.next();
    })

    const sub =
      combineLatest([
        interval(60000).pipe(startWith(-1)),
        this._selectedDate,
        this.ready,
      ])
        .pipe(
          mergeMap(() => this._selectedDate),
          mergeMap(d => {
            const year = d.getFullYear();
            const month = d.getMonth() + 1
            const day = d.getDate();

            return this.calllogapi.forDate(year, month, day);
          }),
          mergeMap(logs => {
            logs = logs || [];

            let customerSet = new Map<string, [string, string]>();
            let distinctNumbers = new Set<string>();

            // get a list of distinct phone number and customer source/ID pairs.
            logs.forEach(l => {
              if (!!l.customerID && !!l.customerSource) {
                customerSet.set(`${l.customerSource}/${l.customerID}`, [
                  l.customerSource,
                  l.customerID
                ]);
              }

              distinctNumbers.add(l.caller.replace(" ", ""));
            })
            let queryForNumbers = Array.from(distinctNumbers.values());

            // make sure we don't send a query if there aren't any
            // numbers we want to lookup as it would return all
            // customers stored in CIS.
            let numbersStream = of([]);
            if (queryForNumbers.length > 0) {
              numbersStream = this.customerapi.search({
                phone: queryForNumbers,
              }).pipe(catchError(err => {
                console.log(err);
                return of([]);
              }));
            }

            let allCustomers: Observable<Customer[]> = of([]);
            if (customerSet.size > 0) {
              allCustomers = combineLatest(
                Array.from(customerSet.values()).map(([source, id]) => {
                  return this.customerapi.byId(source, id)
                    .pipe(catchError(err => of(null)));
                })
              )
            }

            return forkJoin({
              logs: of(logs),
              numbers: numbersStream,
              customers: allCustomers,
            })
          })
        )
        .subscribe(logs => {
          let lm = new Map<string, Customer>();

          logs.numbers?.forEach(cust => cust.phoneNumbers?.forEach(number => {
            number = number.replace(" ", "")
            if (number === "") {
              return
            }

            lm.set(number, cust)
          }))

          this.totalCallTime = sum(
            logs.logs.map(l => l.durationSeconds || 0)
          )

          this.logs = logs.logs.map(l => {
            let cust: Customer | undefined;
            if (!!l.customerSource) {
              cust = logs.customers
                .find(c => !!c && c.source === l.customerSource && c.cid == +l.customerID);
            }

            if (!cust) {
              cust = lm.get(l.caller);
            }

            let d = new Date(l.date);
            return {
              ...l,
              localDate: d.toLocaleDateString(),
              localTime: d.toLocaleTimeString(),
              customer: cust,
              agentUser: this.usersByExtensions.get(l.agent) || this.knownExtensioins.get(l.agent),
              transferToUser: this.usersByExtensions.get(l.transferTarget) || this.knownExtensioins.get(l.transferTarget),
            };
          });
        })

    this.subscriptions.add(sub);
  }

  ngOnDestroy() {
    this.subscriptions.unsubscribe();
  }
}
