import { Component, OnDestroy, OnInit } from "@angular/core";
import { forkJoin, of, Subject } from "rxjs";
import { catchError, debounceTime, switchMap, takeUntil } from "rxjs/operators";
import { CalendarAPI, CalllogAPI, LocalPatient, OpeningHoursAPI, PatientAPI, RosterAPI, UserService } from "src/app/api";
import { Customer, CustomerAPI } from "src/app/api/customer.api";
import { HeaderTitleService } from "src/app/shared/header-title";
import { SelectedTime } from "./quick-time-selector";

interface DisplayCustomer extends Customer {
    display: string;
}

@Component({
    templateUrl: './create-event.html',
    styleUrls: ['./create-event.scss'],
})
export class CreateEventComponent implements OnInit, OnDestroy {
    selectedCustomer: DisplayCustomer | null = null;
    customerSearchResult: DisplayCustomer[] = [];
    calllogSuggestions: DisplayCustomer[] = [];

    selectedPatients: (LocalPatient | string)[] = [];
    customerPatients: LocalPatient[] = [];
    selectedDate: Date | null = new Date();

    selectedTime: SelectedTime | null = null;

    customersLoading = false;
    patientsLoading = false;

    private searchCustomer$ = new Subject<string>();
    private loadPatient$ = new Subject<DisplayCustomer | null>();
    private destroy$ = new Subject<void>();

    constructor(
        private headerService: HeaderTitleService,
        private customerapi: CustomerAPI,
        private calllogapi: CalllogAPI,
        private patientapi: PatientAPI,
        private rosterapi: RosterAPI,
        private calendarapi: CalendarAPI,
        private openinghoursapi: OpeningHoursAPI,
        private users: UserService,
    ) { }

    searchCustomer(name: string) {
        this.searchCustomer$.next(name);
    }

    selectCustomer(customer: DisplayCustomer | null) {
        this.loadPatient$.next(customer);
    }

    selectPatient(event: any) {
        console.log(event);
    }

    nextDay() {
        if (!this.selectedDate) {
            return;
        }
        this.selectedDate = new Date(this.selectedDate.getTime() + 24 * 60 * 60 * 1000);
    }

    prevDay() {
        if (!this.selectedDate) {
            return;
        }
        this.selectedDate = new Date(this.selectedDate.getTime() - 24 * 60 * 60 * 1000);
    }

    disabledDate(d: Date) {
        const now = new Date();
        const midnight = new Date(now.getFullYear(), now.getMonth(), now.getDate());
        return d.getTime() < midnight.getTime();
    }

    ngOnInit() {
        this.headerService.set('Termin eintragen');

        this.calllogapi.forToday()
            .pipe(
                takeUntil(this.destroy$),
                switchMap(calllogs => {
                    // TODO(ppacher): also accept calllogs where we don't know the number and
                    // ask the user to assign it. That's possible the easiest way to get phone - customer
                    // assignments done.
                    let recent = calllogs.filter(call => !!call.customerSource && call.customerSource !== 'unknown').slice(0, 10);
                    return forkJoin(
                        recent.map(call => this.customerapi.byId(call.customerSource!, call.customerID!))
                    );
                })
            )
            .subscribe(result => {
                if (result === null) {
                    return;
                }
                this.calllogSuggestions = result.map(customer => ({
                    ...customer,
                    display: `${customer.name} ${customer.firstname}, ${customer.street}, ${customer.city}`
                }));
                if (this.customerSearchResult.length === 0) {
                    this.customerSearchResult = this.calllogSuggestions;
                }
            })

        this.searchCustomer$.pipe(
            takeUntil(this.destroy$),
            debounceTime(500),
            switchMap(name => {
                this.customersLoading = true;
                if (name === '') {
                    return of(this.calllogSuggestions);
                }
                return this.customerapi.searchName(name);
            }),
        ).subscribe(customers => {
            this.customersLoading = false;
            this.customerSearchResult = customers.map(customer => ({
                ...customer,
                display: `${customer.name} ${customer.firstname}, ${customer.street}, ${customer.city}`
            }));
        });

        this.loadPatient$.pipe(
            takeUntil(this.destroy$),
            debounceTime(500),
            switchMap(customer => {
                this.patientsLoading = true;
                return this.patientapi.getPatientsForCustomer(customer.source, customer.cid);
            }),
            catchError(err => of([] as LocalPatient[]))
        ).subscribe(patients => {
            this.patientsLoading = false;
            this.customerPatients = patients;
        });
    }

    ngOnDestroy() {
        this.destroy$.next();
        this.destroy$.complete();
    }
}